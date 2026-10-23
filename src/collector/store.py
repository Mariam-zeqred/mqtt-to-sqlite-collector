"""SQLite storage: WAL journal, one writer connection, read-only readers, roll-up and compaction.

Tables
  readings  one row per telemetry message (raw), kept RAW_DAYS
  hourly    per node per hour: n, min/mean/max of temp_c and rh_pct, kept HOURLY_DAYS
  status    online/sleeping/offline events, kept RAW_DAYS
  rejected  messages that failed validation, kept 7 days
Time basis for retention is `received` (collector clock, always present); `ts` (device clock) may be null.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .models import Rejected, Status, Telemetry

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
  id        INTEGER PRIMARY KEY,
  node      TEXT    NOT NULL,
  ts        INTEGER,
  received  INTEGER NOT NULL,
  seq       INTEGER NOT NULL,
  temp_c    REAL,
  rh_pct    REAL,
  rssi_dbm  INTEGER,
  uptime_s  INTEGER,
  fw        TEXT
);
CREATE INDEX IF NOT EXISTS readings_node_received ON readings(node, received);
-- QoS 1 redelivery produces exact duplicates; (node, ts, seq) identifies a reading once the clock is synced.
CREATE UNIQUE INDEX IF NOT EXISTS readings_dedupe ON readings(node, ts, seq) WHERE ts IS NOT NULL;
CREATE TABLE IF NOT EXISTS hourly (
  node TEXT NOT NULL, hour INTEGER NOT NULL, n INTEGER NOT NULL,
  temp_min REAL, temp_mean REAL, temp_max REAL, rh_min REAL, rh_mean REAL, rh_max REAL,
  PRIMARY KEY (node, hour)
);
CREATE TABLE IF NOT EXISTS status (
  id INTEGER PRIMARY KEY, node TEXT NOT NULL, state TEXT NOT NULL, ts INTEGER, received INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS status_node_received ON status(node, received);
CREATE TABLE IF NOT EXISTS rejected (
  id INTEGER PRIMARY KEY, received INTEGER NOT NULL, topic TEXT NOT NULL, reason TEXT NOT NULL, payload TEXT
);
"""

ROLLUP = """
INSERT INTO hourly (node, hour, n, temp_min, temp_mean, temp_max, rh_min, rh_mean, rh_max)
SELECT node, (received / 3600) * 3600, COUNT(*),
       MIN(temp_c), AVG(temp_c), MAX(temp_c), MIN(rh_pct), AVG(rh_pct), MAX(rh_pct)
FROM readings WHERE received < ?
GROUP BY node, received / 3600
ON CONFLICT (node, hour) DO UPDATE SET
  temp_mean = (hourly.temp_mean * hourly.n + excluded.temp_mean * excluded.n) / (hourly.n + excluded.n),
  rh_mean   = (hourly.rh_mean * hourly.n + excluded.rh_mean * excluded.n) / (hourly.n + excluded.n),
  temp_min = MIN(hourly.temp_min, excluded.temp_min), temp_max = MAX(hourly.temp_max, excluded.temp_max),
  rh_min   = MIN(hourly.rh_min, excluded.rh_min),     rh_max   = MAX(hourly.rh_max, excluded.rh_max),
  n = hourly.n + excluded.n
"""

DAY = 86_400


def _rows(cur: sqlite3.Cursor) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


class Store:
    """Owns the single writer connection. Call from one thread only (the ingest thread)."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        # One writer connection, shared by paho's network thread (inserts) and the main thread (compact);
        # Ingestor serialises them with a lock, so SQLite's same-thread check is switched off.
        self.conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        # auto_vacuum must be chosen before the first page is written; an older file needs one VACUUM.
        self.conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
        if self.conn.execute("PRAGMA auto_vacuum").fetchone()[0] != 2:
            self.conn.execute("VACUUM")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute(
            "PRAGMA synchronous = NORMAL"
        )  # survives app crashes; power loss may lose the last commit
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    # ---- writes ---------------------------------------------------------------------------------------
    def insert(self, msg: Telemetry | Status | Rejected, received: int) -> str:
        """Store one parsed message. Returns 'stored', 'duplicate' or 'rejected'."""
        if isinstance(msg, Telemetry):
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO readings"
                " (node, ts, received, seq, temp_c, rh_pct, rssi_dbm, uptime_s, fw)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    msg.node,
                    msg.ts,
                    received,
                    msg.seq,
                    msg.temp_c,
                    msg.rh_pct,
                    msg.rssi_dbm,
                    msg.uptime_s,
                    msg.fw,
                ),
            )
            return "stored" if cur.rowcount == 1 else "duplicate"
        if isinstance(msg, Status):
            self.conn.execute(
                "INSERT INTO status (node, state, ts, received) VALUES (?, ?, ?, ?)",
                (msg.node, msg.state, msg.ts, received),
            )
            return "stored"
        self.conn.execute(
            "INSERT INTO rejected (received, topic, reason, payload) VALUES (?, ?, ?, ?)",
            (received, msg.topic, msg.reason, msg.payload),
        )
        return "rejected"

    def compact(
        self, now: int, raw_days: int = 30, hourly_days: int = 730, rejected_days: int = 7
    ) -> dict[str, int]:
        """Roll raw rows older than raw_days into `hourly`, delete expired rows, checkpoint and vacuum.

        The cutoff is aligned to an hour so a bucket is only rolled up once it is complete.
        """
        cutoff = ((now - raw_days * DAY) // 3600) * 3600
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute(ROLLUP, (cutoff,))
            rolled = c.execute("DELETE FROM readings WHERE received < ?", (cutoff,)).rowcount
            status_deleted = c.execute("DELETE FROM status WHERE received < ?", (cutoff,)).rowcount
            hourly_deleted = c.execute(
                "DELETE FROM hourly WHERE hour < ?", (now - hourly_days * DAY,)
            ).rowcount
            rejected_deleted = c.execute(
                "DELETE FROM rejected WHERE received < ?", (now - rejected_days * DAY,)
            ).rowcount
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        # Return the freed pages to the OS. executescript, not execute: Python's sqlite3 steps a row-less
        # PRAGMA only once through execute(), which releases a single page (seen with SQLite 3.45).
        c.executescript("PRAGMA incremental_vacuum;")
        c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return {
            "readings_rolled_up": rolled,
            "status_deleted": status_deleted,
            "hourly_deleted": hourly_deleted,
            "rejected_deleted": rejected_deleted,
            "db_bytes": self.db_bytes(),
        }

    def db_bytes(self) -> int:
        row = self.conn.execute(
            "SELECT page_count * page_size FROM pragma_page_count, pragma_page_size"
        ).fetchone()
        return int(row[0])

    # ---- reads (also used by the API through a separate read-only connection) ------------------------
    def reader(self) -> sqlite3.Connection:
        """A read-only connection; WAL lets it run while the writer inserts."""
        return self.conn if str(self.path) == ":memory:" else open_reader(self.path)


def open_reader(path: Path | str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)


def nodes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn.execute(
            "SELECT r.node, COUNT(*) AS readings, MAX(r.received) AS last_received,"
            " (SELECT state FROM status s WHERE s.node = r.node ORDER BY received DESC, id DESC LIMIT 1)"
            " AS state"
            " FROM readings r GROUP BY r.node ORDER BY r.node"
        )
    )


def latest(conn: sqlite3.Connection, node: str) -> dict[str, Any] | None:
    rows = _rows(
        conn.execute("SELECT * FROM readings WHERE node = ? ORDER BY received DESC, id DESC LIMIT 1", (node,))
    )
    return rows[0] if rows else None


def readings(conn: sqlite3.Connection, node: str, since: int, until: int, limit: int) -> list[dict[str, Any]]:
    return _rows(
        conn.execute(
            "SELECT * FROM readings WHERE node = ? AND received >= ? AND received < ?"
            " ORDER BY received, id LIMIT ?",
            (node, since, until, limit),
        )
    )


def hourly(conn: sqlite3.Connection, node: str, since: int, until: int, limit: int) -> list[dict[str, Any]]:
    return _rows(
        conn.execute(
            "SELECT * FROM hourly WHERE node = ? AND hour >= ? AND hour < ? ORDER BY hour LIMIT ?",
            (node, since, until, limit),
        )
    )


def rejected(conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
    return _rows(conn.execute("SELECT * FROM rejected ORDER BY id DESC LIMIT ?", (limit,)))


def stats(conn: sqlite3.Connection) -> dict[str, Any]:
    counts = {
        t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in ("readings", "hourly", "status", "rejected")
    }
    size = conn.execute("SELECT page_count * page_size FROM pragma_page_count, pragma_page_size").fetchone()[
        0
    ]
    first, last = conn.execute("SELECT MIN(received), MAX(received) FROM readings").fetchone()
    return {"rows": counts, "db_bytes": int(size), "first_received": first, "last_received": last}
