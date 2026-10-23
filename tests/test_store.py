import sqlite3
import statistics

import pytest

from collector.models import Rejected, Status, Telemetry
from collector.store import DAY, Store, hourly, latest, nodes, open_reader, readings, stats

T0 = 1_791_000_000  # 2026-10-03, aligned to an hour? no: we align explicitly below
H0 = (T0 // 3600) * 3600


def reading(node="esp32-a", ts=T0, seq=0, temp=30.0, rh=50.0):
    return Telemetry(
        node=node, ts=ts, seq=seq, temp_c=temp, rh_pct=rh, rssi_dbm=-60, uptime_s=seq * 30, fw="0.1.0"
    )


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "t.db")
    yield s
    s.close()


def test_wal_mode_and_schema(store):
    assert store.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert store.conn.execute("PRAGMA auto_vacuum").fetchone()[0] == 2  # incremental
    tables = {r[0] for r in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"readings", "hourly", "status", "rejected"} <= tables


def test_insert_and_duplicate_detection(store):
    assert store.insert(reading(seq=1), T0) == "stored"
    assert store.insert(reading(seq=1), T0 + 5) == "duplicate"  # QoS 1 redelivery
    assert store.insert(reading(seq=2), T0) == "stored"
    # Without a synced clock there is no identity, so nothing is dropped.
    assert store.insert(Telemetry(node="esp32-a", ts=None, seq=9, temp_c=1), T0) == "stored"
    assert store.insert(Telemetry(node="esp32-a", ts=None, seq=9, temp_c=1), T0) == "stored"
    assert store.conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 4


def test_status_and_rejected_rows(store):
    assert store.insert(Status(node="esp32-a", state="online", ts=T0), T0) == "stored"
    assert store.insert(Rejected(topic="farm/x/telemetry", reason="not JSON", payload="?"), T0) == "rejected"
    s = stats(store.conn)
    assert s["rows"] == {"readings": 0, "hourly": 0, "status": 1, "rejected": 1}


def test_queries(store):
    for i in range(10):
        store.insert(reading(seq=i, ts=T0 + 30 * i, temp=20 + i), T0 + 30 * i)
    store.insert(reading(node="esp32-b", seq=0), T0)
    store.insert(Status(node="esp32-b", state="sleeping"), T0 + 1)
    assert latest(store.conn, "esp32-a")["temp_c"] == 29
    assert latest(store.conn, "nope") is None
    rows = readings(store.conn, "esp32-a", T0 + 60, T0 + 150, 100)
    assert [r["seq"] for r in rows] == [2, 3, 4]
    assert len(readings(store.conn, "esp32-a", 0, 2**40, 3)) == 3
    ns = nodes(store.conn)
    assert [n["node"] for n in ns] == ["esp32-a", "esp32-b"]
    assert ns[0]["readings"] == 10 and ns[0]["state"] is None and ns[1]["state"] == "sleeping"


def test_compact_rolls_up_complete_hours_only_and_deletes_raw(store):
    temps = []
    for i in range(240):  # 2 h of 30 s readings, starting on an hour boundary
        t = 25 + (i % 7)
        temps.append(t)
        store.insert(reading(seq=i, ts=H0 + 30 * i, temp=t, rh=40 + i % 3), H0 + 30 * i)
    # One recent reading that must survive.
    now = H0 + 2 * 3600 + 31 * DAY
    store.insert(reading(seq=999, ts=now - 10), now - 10)

    result = store.compact(now=now, raw_days=30)
    assert result["readings_rolled_up"] == 240
    left = readings(store.conn, "esp32-a", 0, 2**40, 100)
    assert [r["seq"] for r in left] == [999]
    hrs = hourly(store.conn, "esp32-a", 0, 2**40, 10)
    assert [h["hour"] for h in hrs] == [H0, H0 + 3600]
    assert hrs[0]["n"] == 120 and hrs[1]["n"] == 120
    assert hrs[0]["temp_min"] == 25 and hrs[0]["temp_max"] == 31
    assert hrs[0]["temp_mean"] == pytest.approx(statistics.mean(temps[:120]))
    assert hrs[0]["rh_min"] == 40 and hrs[0]["rh_max"] == 42


def test_compact_cutoff_is_hour_aligned_so_partial_hours_wait(store):
    # Readings in one hour; "now" is such that raw_days ago falls in the middle of that hour.
    for i in range(120):
        store.insert(reading(seq=i, ts=H0 + 30 * i), H0 + 30 * i)
    now = H0 + 1800 + 30 * DAY  # cutoff (now - 30 d) = H0 + 1800, aligned down to H0 -> nothing rolled up
    assert store.compact(now=now, raw_days=30)["readings_rolled_up"] == 0
    assert store.compact(now=now + 1800, raw_days=30)["readings_rolled_up"] == 120


def test_rollup_merges_when_an_hour_is_rolled_up_twice(store):
    for i in range(60):
        store.insert(reading(seq=i, ts=H0 + 30 * i, temp=10), H0 + 30 * i)
    store.compact(now=H0 + 3600 + 30 * DAY, raw_days=30)
    # Late-arriving rows for the same hour (e.g. a node that buffered while offline).
    for i in range(60, 120):
        store.insert(reading(seq=i, ts=H0 + 30 * i, temp=20), H0 + 30 * i)
    store.compact(now=H0 + 3600 + 30 * DAY, raw_days=30)
    (h,) = hourly(store.conn, "esp32-a", 0, 2**40, 10)
    assert (
        h["n"] == 120 and h["temp_mean"] == pytest.approx(15) and h["temp_min"] == 10 and h["temp_max"] == 20
    )


def test_compact_expires_hourly_status_and_rejected(store):
    old = T0 - 800 * DAY
    store.insert(reading(ts=old), old)
    store.insert(Status(node="esp32-a", state="online"), old)
    store.insert(Rejected(topic="t", reason="r", payload="p"), T0 - 8 * DAY)
    store.insert(Rejected(topic="t", reason="r", payload="p"), T0 - 1 * DAY)
    r1 = store.compact(now=T0, raw_days=30, hourly_days=730)
    assert r1["readings_rolled_up"] == 1 and r1["status_deleted"] == 1 and r1["rejected_deleted"] == 1
    # the hourly row created from the 800-day-old reading is itself older than hourly_days
    assert r1["hourly_deleted"] == 1
    assert stats(store.conn)["rows"] == {"readings": 0, "hourly": 0, "status": 0, "rejected": 1}


def test_reader_sees_writes_while_writer_is_open(store):
    rd = open_reader(store.path)
    store.insert(reading(seq=1), T0)
    assert rd.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 1
    with pytest.raises(sqlite3.OperationalError):
        rd.execute("DELETE FROM readings")
    rd.close()


def test_compact_shrinks_the_file(store):
    for i in range(5000):
        store.insert(reading(seq=i, ts=T0 + 30 * i), T0 + 30 * i)
    before = store.db_bytes()
    after = store.compact(now=T0 + 5000 * 30 + 40 * DAY, raw_days=30)["db_bytes"]
    assert after < before / 2
