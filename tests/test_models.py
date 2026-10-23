import json

from collector.models import Rejected, Status, Telemetry, parse_message, topic_parts

NODE_DOC = {
    "node": "esp32-a1b2c3",
    "ts": 1791676800,
    "seq": 42,
    "temp_c": 36.52,
    "rh_pct": 61.25,
    "rssi_dbm": -67,
    "uptime_s": 1234,
    "fw": "0.1.0",
}
TOPIC = "farm/esp32-a1b2c3/telemetry"


def msg(doc, topic=TOPIC):
    return parse_message(topic, json.dumps(doc).encode())


def test_firmware_document_parses_exactly():
    t = msg(NODE_DOC)
    assert isinstance(t, Telemetry)
    assert t.model_dump() == NODE_DOC


def test_nulls_from_firmware_are_kept_as_none():
    t = msg({**NODE_DOC, "ts": None, "temp_c": None, "rh_pct": None})
    assert isinstance(t, Telemetry)
    assert t.ts is None and t.temp_c is None and t.rh_pct is None


def test_nan_and_infinity_become_none():
    t = parse_message(TOPIC, b'{"node":"esp32-a1b2c3","seq":1,"temp_c":NaN,"rh_pct":Infinity}')
    assert isinstance(t, Telemetry)
    assert t.temp_c is None and t.rh_pct is None


def test_missing_optional_fields_are_fine_unknown_keys_ignored():
    t = msg({"node": "esp32-a1b2c3", "seq": 7, "temp_c": 30, "rh_pct": 50, "heater": True, "new_field": 1})
    assert isinstance(t, Telemetry)
    assert t.rssi_dbm is None and t.fw is None
    assert not hasattr(t, "heater")


def test_out_of_range_values_are_rejected_with_a_reason():
    r = msg({**NODE_DOC, "temp_c": 130.0})
    assert isinstance(r, Rejected)
    assert r.reason.startswith("temp_c:")
    r = msg({**NODE_DOC, "rh_pct": -1})
    assert isinstance(r, Rejected) and r.reason.startswith("rh_pct:")
    r = msg({**NODE_DOC, "ts": 12345})  # 1970: clock never synced but not null
    assert isinstance(r, Rejected) and r.reason.startswith("ts:")


def test_missing_required_seq_is_rejected():
    doc = dict(NODE_DOC)
    del doc["seq"]
    r = msg(doc)
    assert isinstance(r, Rejected) and "seq" in r.reason


def test_node_in_payload_must_match_topic():
    r = msg({**NODE_DOC, "node": "esp32-other0"})
    assert isinstance(r, Rejected) and "!=" in r.reason


def test_bad_topics_and_payloads():
    assert isinstance(parse_message("other/esp32-a1/telemetry", b"{}"), Rejected)
    assert isinstance(parse_message("farm/esp32-a1/telemetry/extra", b"{}"), Rejected)
    assert isinstance(parse_message("farm//telemetry", b"{}"), Rejected)
    assert parse_message(TOPIC, b"not json").reason == "not JSON"
    assert parse_message(TOPIC, b"[1,2]").reason == "JSON is not an object"
    assert parse_message(TOPIC, b"\xff\xfe").reason == "not JSON"
    big = parse_message(TOPIC, b"{" + b" " * 2000 + b"}")
    assert isinstance(big, Rejected) and "1024" in big.reason and len(big.payload) <= 200


def test_prefix_is_configurable():
    t = parse_message("barn/esp32-a1b2c3/telemetry", json.dumps(NODE_DOC).encode(), prefix="barn")
    assert isinstance(t, Telemetry)
    assert topic_parts("barn/x/status", "barn") == ("x", "status")
    assert topic_parts("barn/x/status", "farm") is None


def test_status_documents():
    s = parse_message("farm/esp32-a1b2c3/status", b'{"node":"esp32-a1b2c3","state":"offline"}')
    assert isinstance(s, Status) and s.state == "offline" and s.ts is None
    s = parse_message("farm/esp32-a1b2c3/status", b'{"node":"esp32-a1b2c3","state":"online","ts":1791676800}')
    assert isinstance(s, Status) and s.ts == 1791676800
    r = parse_message("farm/esp32-a1b2c3/status", b'{"node":"esp32-a1b2c3","state":"rebooting"}')
    assert isinstance(r, Rejected) and r.reason.startswith("state:")
