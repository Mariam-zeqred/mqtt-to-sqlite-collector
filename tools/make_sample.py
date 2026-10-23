#!/usr/bin/env python3
"""Convert the real incubator history from incubator-telemetry-analysis into the sensor node's MQTT format.

    python tools/make_sample.py ../incubator-telemetry-analysis/data/hive-1C5A00_history_2026-10-03.json \
        data/sample_incubator.jsonl

Each output line is {"topic": "farm/<node>/telemetry", "payload": {...}} for `python -m collector replay`.
Only fields the logger really recorded are copied: ts, temp (-> temp_c), hum (-> rh_pct). The node firmware
also sends rssi_dbm, uptime_s and fw; those were not logged, so they are left out rather than invented.
`seq` is the reading's index in the file.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def convert(src: Path, dst: Path, node: str = "hive-1C5A00", prefix: str = "farm") -> int:
    rows = json.loads(src.read_text(encoding="utf-8"))
    rows.sort(key=lambda r: r["ts"])
    with dst.open("w", encoding="utf-8") as out:
        first = {
            "topic": f"{prefix}/{node}/status",
            "payload": {"node": node, "state": "online", "ts": rows[0]["ts"]},
        }
        out.write(json.dumps(first) + "\n")
        for i, r in enumerate(rows):
            payload = {"node": node, "ts": r["ts"], "seq": i, "temp_c": r["temp"], "rh_pct": r["hum"]}
            out.write(json.dumps({"topic": f"{prefix}/{node}/telemetry", "payload": payload}) + "\n")
    return len(rows)


if __name__ == "__main__":
    n = convert(Path(sys.argv[1]), Path(sys.argv[2]))
    print(f"wrote {n} readings + 1 status to {sys.argv[2]}")
