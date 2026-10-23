import pytest
from starlette.testclient import TestClient

from collector.api import create_app
from collector.models import Status, Telemetry
from collector.store import DAY, Store

T0 = 1_780_000_000  # 2026-05-28: T0 + 100 days is still in the past when the API computes ages


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "api.db"
    s = Store(path)
    for i in range(50):
        s.insert(
            Telemetry(node="esp32-a", ts=T0 + 30 * i, seq=i, temp_c=20 + i * 0.1, rh_pct=50), T0 + 30 * i
        )
    s.insert(Status(node="esp32-a", state="online", ts=T0), T0)
    s.insert(Telemetry(node="esp32-b", ts=T0, seq=0, temp_c=99.5), T0)
    s.compact(now=T0 + 100 * DAY, raw_days=30)  # everything rolled up to hourly ...
    s.insert(Telemetry(node="esp32-a", ts=T0 + 100 * DAY, seq=51, temp_c=33.3), T0 + 100 * DAY)  # ... but one
    s.close()
    return path


@pytest.fixture
def client(db):
    return TestClient(create_app(db))


def test_health_and_stats(client):
    h = client.get("/health").json()
    assert h["status"] == "ok" and h["readings"] == 1 and h["last_reading_age_s"] > 0
    s = client.get("/stats").json()
    assert s["rows"]["hourly"] == 2 and s["rows"]["status"] == 0 and s["db_bytes"] > 0


def test_nodes_and_latest(client):
    assert [n["node"] for n in client.get("/nodes").json()] == ["esp32-a"]
    assert client.get("/nodes/esp32-a/latest").json()["temp_c"] == 33.3
    assert client.get("/nodes/esp32-b/latest").status_code == 404  # its raw row was rolled up


def test_readings_window_and_limit(client):
    assert client.get("/nodes/esp32-a/readings").json() == []  # default window is the last 24 h of wall clock
    rows = client.get("/nodes/esp32-a/readings", params={"since": 0, "until": 2**40}).json()
    assert len(rows) == 1 and rows[0]["seq"] == 51
    assert client.get("/nodes/esp32-a/readings", params={"limit": 0}).status_code == 422
    assert client.get("/nodes/esp32-a/readings", params={"limit": 10**6}).status_code == 422


def test_hourly(client):
    rows = client.get("/nodes/esp32-a/hourly", params={"since": 0, "until": 2**40}).json()
    assert len(rows) == 1 and rows[0]["n"] == 50
    assert rows[0]["temp_min"] == pytest.approx(20.0) and rows[0]["temp_max"] == pytest.approx(24.9)
    assert (
        client.get("/nodes/esp32-b/hourly", params={"since": 0, "until": 2**40}).json()[0]["temp_max"] == 99.5
    )


def test_rejected_endpoint_and_missing_db(tmp_path, client):
    assert client.get("/rejected").json() == []
    assert TestClient(create_app(tmp_path / "nope.db")).get("/health").status_code == 503
