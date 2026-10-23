"""No Grafana runs here, but every query in the dashboard can be executed against the real schema."""

import json
import re
from pathlib import Path

from collector.models import Telemetry
from collector.store import Store

DASHBOARD = Path("grafana/dashboard.json")


def _sql_for_sqlite(sql: str) -> str:
    """Replace Grafana's macros and the ${node} variable with literal values so SQLite can run the query."""
    sql = sql.replace("$__from", "0").replace("$__to", "9000000000000")
    return re.sub(r"\$\{node\}", "esp32-a", sql)


def test_every_dashboard_query_runs_against_the_schema(tmp_path):
    doc = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    store = Store(tmp_path / "g.db")
    store.insert(
        Telemetry(node="esp32-a", ts=1_791_000_000, seq=1, temp_c=30, rh_pct=50, rssi_dbm=-60), 1_791_000_000
    )
    store.compact(now=1_791_000_000 + 40 * 86_400)
    store.insert(Telemetry(node="esp32-a", ts=1_791_000_001, seq=2, temp_c=31), 1_791_000_001)

    queries = [t["rawQueryText"] for p in doc["panels"] for t in p["targets"]]
    queries.append(doc["templating"]["list"][0]["query"]["queryText"])
    assert len(queries) == 8
    for sql in queries:
        cur = store.conn.execute(_sql_for_sqlite(sql))
        assert cur.fetchall(), sql  # each query returns at least one row on this tiny database
    for p in doc["panels"]:
        if p["type"] == "timeseries":
            assert p["targets"][0]["timeColumns"] == ["time"]
            assert " AS time" in p["targets"][0]["rawQueryText"]
    assert len({p["id"] for p in doc["panels"]}) == len(doc["panels"])
