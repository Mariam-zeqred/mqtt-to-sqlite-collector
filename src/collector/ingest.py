"""MQTT subscription -> parse -> store. paho-mqtt does the reconnect backoff; we count what happened."""

from __future__ import annotations

import logging
import ssl
import threading
import time
from collections import Counter

import paho.mqtt.client as mqtt

from .config import Settings
from .models import Rejected, parse_message
from .store import Store

log = logging.getLogger("collector.ingest")


class Ingestor:
    """Everything that touches the Store happens in `handle`, which paho calls from its network thread.

    `handle` is also the seam the tests use: they call it directly, or through a fake client.
    """

    def __init__(self, settings: Settings, store: Store) -> None:
        self.settings = settings
        self.store = store
        self.counts: Counter[str] = Counter()
        self.topics = [(f"{settings.mqtt_prefix}/+/telemetry", 1), (f"{settings.mqtt_prefix}/+/status", 1)]
        self._lock = threading.Lock()  # handle() and the periodic compact() share the writer connection

    def handle(self, topic: str, payload: bytes, received: int | None = None) -> str:
        received = int(time.time()) if received is None else received
        msg = parse_message(topic, payload, self.settings.mqtt_prefix)
        with self._lock:
            outcome = self.store.insert(msg, received)
        self.counts[outcome] += 1
        if isinstance(msg, Rejected):
            log.warning("rejected %s: %s", topic, msg.reason)
        return outcome

    def compact(self, now: int | None = None) -> dict[str, int]:
        now = int(time.time()) if now is None else now
        with self._lock:
            result = self.store.compact(now, self.settings.raw_days, self.settings.hourly_days)
        log.info("compact: %s", result)
        return result

    # ---- paho wiring --------------------------------------------------------------------------------
    def build_client(self) -> mqtt.Client:
        s = self.settings
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=f"collector-{s.mqtt_prefix}", clean_session=False
        )
        if s.mqtt_user:
            client.username_pw_set(s.mqtt_user, s.mqtt_password or None)
        if s.mqtt_ca:
            client.tls_set(
                ca_certs=s.mqtt_ca, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT
            )
        client.reconnect_delay_set(min_delay=1, max_delay=60)  # exponential backoff between retries
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        return client

    def _on_connect(
        self, client: mqtt.Client, _ud: object, _flags: object, rc: object, _props: object = None
    ) -> None:
        log.info("connected to %s:%d (%s)", self.settings.mqtt_host, self.settings.mqtt_port, rc)
        client.subscribe(self.topics)  # re-subscribe after every reconnect (persistent session or not)
        self.counts["connects"] += 1

    def _on_disconnect(
        self, _client: mqtt.Client, _ud: object, _flags: object, rc: object, _props: object = None
    ) -> None:
        log.warning("disconnected (%s); paho will retry with backoff", rc)
        self.counts["disconnects"] += 1

    def _on_message(self, _client: mqtt.Client, _ud: object, message: mqtt.MQTTMessage) -> None:
        try:
            self.handle(message.topic, message.payload)
        except Exception:  # a bad row must never kill the network thread
            log.exception("failed to store message on %s", message.topic)
            self.counts["errors"] += 1

    def run_forever(self, stop: threading.Event) -> None:
        """Connect (retrying), serve messages, compact every COMPACT_EVERY_S, return when `stop` is set."""
        client = self.build_client()
        client.connect_async(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=60)
        client.loop_start()
        next_compact = time.monotonic() + self.settings.compact_every_s
        try:
            while not stop.wait(1.0):
                if time.monotonic() >= next_compact:
                    self.compact()
                    next_compact = time.monotonic() + self.settings.compact_every_s
        finally:
            client.disconnect()
            client.loop_stop()
