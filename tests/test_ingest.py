import json
from types import SimpleNamespace

from collector.config import Settings
from collector.ingest import Ingestor
from collector.store import Store, stats

T0 = 1_791_000_000


def make(tmp_path, **kw):
    settings = Settings(mqtt_prefix="farm", raw_days=30, **kw)
    store = Store(tmp_path / "i.db")
    return Ingestor(settings, store), store


def test_handle_counts_outcomes(tmp_path):
    ing, store = make(tmp_path)
    doc = {"node": "esp32-a", "ts": T0, "seq": 1, "temp_c": 30, "rh_pct": 50}
    assert ing.handle("farm/esp32-a/telemetry", json.dumps(doc).encode(), T0) == "stored"
    assert ing.handle("farm/esp32-a/telemetry", json.dumps(doc).encode(), T0 + 1) == "duplicate"
    assert ing.handle("farm/esp32-a/status", b'{"node":"esp32-a","state":"online"}', T0) == "stored"
    assert ing.handle("farm/esp32-a/telemetry", b"garbage", T0) == "rejected"
    assert dict(ing.counts) == {"stored": 2, "duplicate": 1, "rejected": 1}
    assert stats(store.conn)["rows"] == {"readings": 1, "hourly": 0, "status": 1, "rejected": 1}


def test_on_message_never_raises(tmp_path):
    ing, store = make(tmp_path)
    store.close()  # every insert will now fail with ProgrammingError
    msg = SimpleNamespace(topic="farm/esp32-a/telemetry", payload=b'{"node":"esp32-a","seq":1}')
    ing._on_message(None, None, msg)
    assert ing.counts["errors"] == 1


def test_fake_client_round_trip(tmp_path):
    """Drive the paho callbacks by hand with a fake client: subscribe on connect, store on message."""
    ing, store = make(tmp_path)
    subscribed = []
    fake = SimpleNamespace(subscribe=lambda topics: subscribed.append(topics))
    ing._on_connect(fake, None, None, 0)
    assert subscribed == [[("farm/+/telemetry", 1), ("farm/+/status", 1)]]
    for seq in range(3):
        payload = json.dumps({"node": "esp32-z", "ts": T0 + seq, "seq": seq, "temp_c": 21.5}).encode()
        ing._on_message(fake, None, SimpleNamespace(topic="farm/esp32-z/telemetry", payload=payload))
    ing._on_disconnect(fake, None, None, 0)
    assert ing.counts["stored"] == 3 and ing.counts["connects"] == 1 and ing.counts["disconnects"] == 1


def test_build_client_applies_settings(tmp_path):
    ing, _ = make(tmp_path, mqtt_user="collector", mqtt_password="pw")
    client = ing.build_client()
    assert client._username == b"collector" and client._client_id == b"collector-farm"
    assert client._reconnect_max_delay == 60


def test_compact_through_ingestor(tmp_path):
    ing, store = make(tmp_path)
    for seq in range(10):
        ing.handle(
            "farm/esp32-a/telemetry",
            json.dumps({"node": "esp32-a", "ts": T0 + seq, "seq": seq}).encode(),
            T0 + seq,
        )
    result = ing.compact(now=T0 + 40 * 86_400)
    assert result["readings_rolled_up"] == 10 and stats(store.conn)["rows"]["hourly"] == 1
