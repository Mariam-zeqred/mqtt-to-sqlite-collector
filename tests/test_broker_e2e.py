"""End-to-end: a real MQTT broker (amqtt, in-process), the collector's own paho client, a paho publisher."""

import asyncio
import json
import socket
import threading
import time

import paho.mqtt.client as mqtt
import pytest

from collector.config import Settings
from collector.ingest import Ingestor
from collector.store import Store, latest, stats

amqtt_broker = pytest.importorskip("amqtt.broker")

T0 = 1_791_000_000


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def broker_port():
    port = _free_port()
    config = {
        "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{port}"}},
        "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
    }
    loop = asyncio.new_event_loop()
    broker = amqtt_broker.Broker(config, loop=loop)
    started = threading.Event()

    def run() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(broker.start())
        started.set()
        loop.run_forever()

    threading.Thread(target=run, daemon=True).start()
    assert started.wait(10), "amqtt broker did not start"
    yield port
    asyncio.run_coroutine_threadsafe(broker.shutdown(), loop)
    loop.call_soon_threadsafe(loop.stop)


def _wait(predicate, timeout=5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def test_collector_stores_what_a_node_publishes(tmp_path, broker_port):
    settings = Settings(mqtt_host="127.0.0.1", mqtt_port=broker_port, mqtt_prefix="farm")
    store = Store(tmp_path / "e2e.db")
    ing = Ingestor(settings, store)
    client = ing.build_client()
    client.connect(settings.mqtt_host, settings.mqtt_port)
    client.loop_start()
    assert _wait(lambda: ing.counts["connects"] >= 1)

    node = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="esp32-e2e")
    node.connect("127.0.0.1", broker_port)
    node.loop_start()
    time.sleep(0.3)  # let the collector's SUBSCRIBE land before publishing
    node.publish("farm/esp32-e2e/status", json.dumps({"node": "esp32-e2e", "state": "online"}), qos=1)
    for seq in range(5):
        doc = {
            "node": "esp32-e2e",
            "ts": T0 + seq,
            "seq": seq,
            "temp_c": 30 + seq,
            "rh_pct": 55,
            "rssi_dbm": -60,
        }
        node.publish("farm/esp32-e2e/telemetry", json.dumps(doc), qos=1).wait_for_publish(5)
    node.publish(
        "farm/esp32-e2e/telemetry", json.dumps({"node": "esp32-e2e", "ts": T0, "seq": 0}), qos=1
    )  # dup
    node.publish("farm/esp32-e2e/telemetry", b"not json", qos=1)
    node.publish(
        "farm/esp32-e2e/telemetry", json.dumps({"node": "esp32-e2e", "seq": 6, "temp_c": 500}), qos=1
    )
    node.publish("other/esp32-e2e/telemetry", b"{}", qos=1).wait_for_publish(5)  # not subscribed, never seen

    assert _wait(lambda: ing.counts["stored"] + ing.counts["duplicate"] + ing.counts["rejected"] >= 9), (
        ing.counts
    )
    node.loop_stop()
    node.disconnect()
    client.loop_stop()
    client.disconnect()

    assert dict(ing.counts) == {"connects": 1, "disconnects": 1, "stored": 6, "duplicate": 1, "rejected": 2}
    assert stats(store.conn)["rows"] == {"readings": 5, "hourly": 0, "status": 1, "rejected": 2}
    assert latest(store.conn, "esp32-e2e")["temp_c"] == 34
    store.close()
