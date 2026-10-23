#!/usr/bin/env python3
"""Generate grafana/dashboard.json for the frser-sqlite-datasource plugin (valid, reviewable JSON)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

DS = {"type": "frser-sqlite-datasource", "uid": "telemetry-sqlite"}
WINDOW = "received >= $__from / 1000 AND received < $__to / 1000"
RAW = (
    "SELECT received AS time, {col} FROM readings WHERE node = '${{node}}' AND "
    + WINDOW
    + " ORDER BY received"
)


def target(sql: str, time_columns: list[str] | None = None) -> dict:
    return {
        "datasource": DS,
        "refId": "A",
        "queryType": "table",
        "rawQueryText": sql,
        "queryText": sql,
        "timeColumns": time_columns or [],
    }


def timeseries(pid: int, title: str, sql: str, unit: str, x: int, y: int, w: int = 12, h: int = 8) -> dict:
    return {
        "id": pid,
        "type": "timeseries",
        "title": title,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"spanNulls": 120000}}, "overrides": []},
        "targets": [target(sql, ["time"])],
    }


panels = [
    timeseries(
        1,
        "Temperature (raw, °C)",
        RAW.format(col="temp_c"),
        "celsius",
        0,
        0,
    ),
    timeseries(
        2,
        "Relative humidity (raw, %)",
        RAW.format(col="rh_pct"),
        "humidity",
        12,
        0,
    ),
    timeseries(
        3,
        "Hourly temperature min / mean / max (rolled-up history)",
        "SELECT hour AS time, temp_min, temp_mean, temp_max FROM hourly"
        " WHERE node = '${node}' AND hour >= $__from / 1000 AND hour < $__to / 1000 ORDER BY hour",
        "celsius",
        0,
        8,
    ),
    timeseries(
        4,
        "Wi-Fi RSSI (dBm)",
        RAW.format(col="rssi_dbm"),
        "dBm",
        12,
        8,
    ),
    {
        "id": 5,
        "type": "table",
        "title": "Nodes",
        "datasource": DS,
        "gridPos": {"x": 0, "y": 16, "w": 12, "h": 7},
        "targets": [
            target(
                "SELECT node, COUNT(*) AS readings, datetime(MAX(received), 'unixepoch') AS last_seen,"
                " (SELECT state FROM status s WHERE s.node = r.node ORDER BY received DESC LIMIT 1) AS state"
                " FROM readings r GROUP BY node ORDER BY node"
            )
        ],
    },
    {
        "id": 6,
        "type": "stat",
        "title": "Rejected messages in the selected range",
        "datasource": DS,
        "gridPos": {"x": 12, "y": 16, "w": 6, "h": 7},
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}],
                },
            },
            "overrides": [],
        },
        "targets": [target(f"SELECT COUNT(*) AS rejected FROM rejected WHERE {WINDOW}")],
    },  # fmt: skip
    {
        "id": 7,
        "type": "stat",
        "title": "Database size",
        "datasource": DS,
        "gridPos": {"x": 18, "y": 16, "w": 6, "h": 7},
        "fieldConfig": {"defaults": {"unit": "bytes"}, "overrides": []},
        "targets": [
            target("SELECT page_count * page_size AS db_bytes FROM pragma_page_count, pragma_page_size")
        ],
    },
]

dashboard = {
    "uid": "mqtt-sqlite-collector",
    "title": "MQTT → SQLite collector",
    "tags": ["iot", "sqlite", "mqtt"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "refresh": "30s",
    "time": {"from": "now-6h", "to": "now"},
    "templating": {
        "list": [
            {
                "name": "node",
                "label": "Node",
                "type": "query",
                "datasource": DS,
                "query": {
                    "queryText": "SELECT DISTINCT node FROM readings ORDER BY node",
                    "rawQueryText": True,
                },
                "refresh": 2,
                "includeAll": False,
                "multi": False,
                "sort": 1,
            }
        ]
    },
    "panels": panels,
}

if __name__ == "__main__":
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "grafana/dashboard.json")
    out.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {out} with {len(panels)} panels")
