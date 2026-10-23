#!/usr/bin/env python3
"""Publish telemetry to a broker in the esp32-mqtt-sensor-node format, for demos without hardware.

    python tools/publish_fake_node.py --host localhost --node esp32-demo01 --period 2 --count 30
    python tools/publish_fake_node.py --host localhost --replay data/sample_incubator.jsonl --speed 60

Without --replay the values are SYNTHETIC (a slow sine on temperature, noise on humidity) and say so in `fw`.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import time

import paho.mqtt.client as mqtt


def synthetic(node: str, seq: int, now: int) -> dict:
    return {
        "node": node,
        "ts": now,
        "seq": seq,
        "temp_c": round(32 + 2 * math.sin(seq / 20) + random.gauss(0, 0.05), 2),
        "rh_pct": round(60 + random.gauss(0, 1), 2),
        "rssi_dbm": random.randrange(-75, -55),
        "uptime_s": seq * 30,
        "fw": "synthetic",
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default=os.environ.get("MQTT_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    ap.add_argument("--user", default=os.environ.get("MQTT_USER", ""))
    ap.add_argument("--password", default=os.environ.get("MQTT_PASSWORD", ""))
    ap.add_argument("--prefix", default=os.environ.get("MQTT_PREFIX", "farm"))
    ap.add_argument("--node", default="esp32-demo01")
    ap.add_argument("--period", type=float, default=2.0, help="seconds between synthetic readings")
    ap.add_argument("--count", type=int, default=30)
    ap.add_argument(
        "--replay", help="JSON-lines file from tools/make_sample.py; topics are taken from the file"
    )
    ap.add_argument("--speed", type=float, default=0, help="replay speed factor, 0 = no sleeping")
    args = ap.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if args.user:
        client.username_pw_set(args.user, args.password or None)
    client.connect(args.host, args.port)
    client.loop_start()
    try:
        if args.replay:
            prev = None
            for line in open(args.replay, encoding="utf-8"):
                item = json.loads(line)
                ts = item["payload"].get("ts")
                if args.speed > 0 and prev is not None and ts is not None:
                    time.sleep(max(0, ts - prev) / args.speed)
                prev = ts if ts is not None else prev
                client.publish(item["topic"], json.dumps(item["payload"]), qos=1).wait_for_publish(5)
            return
        topic = f"{args.prefix}/{args.node}/telemetry"
        client.publish(
            f"{args.prefix}/{args.node}/status", json.dumps({"node": args.node, "state": "online"}), qos=1
        )
        for seq in range(args.count):
            doc = synthetic(args.node, seq, int(time.time()))
            client.publish(topic, json.dumps(doc), qos=1).wait_for_publish(5)
            print(topic, json.dumps(doc))
            time.sleep(args.period)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
