"""Retention arithmetic for the README: how big does the SQLite file get for N nodes?

Row sizes are *measured* by inserting rows into a temporary database (indexes included), not guessed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path

from .models import Telemetry
from .store import DAY, Store


@dataclass(frozen=True)
class RowSizes:
    reading_bytes: float  # bytes per raw reading including its two indexes
    hourly_bytes: float  # bytes per hourly aggregate row


def measure_row_sizes(tmp_dir: Path, n: int = 20_000, seed: int = 1) -> RowSizes:
    """Insert n realistic readings for 10 nodes, roll all of them up, and read the page counts."""
    rng = random.Random(seed)
    store = Store(tmp_dir / "measure.db")
    empty_bytes = store.db_bytes()  # schema pages, subtracted from the small hourly table below
    base = 1_791_000_000
    for i in range(n):
        node = f"esp32-{rng.randrange(10):06x}"
        msg = Telemetry(
            node=node,
            ts=base + i * 30,
            seq=i,
            temp_c=round(rng.uniform(20, 40), 2),
            rh_pct=round(rng.uniform(30, 90), 2),
            rssi_dbm=rng.randrange(-90, -40),
            uptime_s=i * 30,
            fw="0.1.0",
        )
        store.insert(msg, base + i * 30)
    store.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    raw_bytes = store.db_bytes()
    # Roll everything up (cutoff far in the future) and measure the hourly table alone.
    hours = store.conn.execute(
        "SELECT COUNT(DISTINCT node || '/' || (received / 3600)) FROM readings"
    ).fetchone()[0]
    store.compact(now=base + n * 30 + 400 * DAY, raw_days=0, hourly_days=100_000)
    hourly_bytes = store.db_bytes() - empty_bytes
    store.close()
    return RowSizes(reading_bytes=raw_bytes / n, hourly_bytes=hourly_bytes / max(hours, 1))


@dataclass(frozen=True)
class Plan:
    nodes: int
    rows_per_day: int
    raw_mb: float  # steady-state size of the raw table after raw_days
    hourly_mb_per_year: float
    total_mb: float  # raw at steady state + hourly after hourly_days


def plan(nodes: int, sizes: RowSizes, period_s: int = 30, raw_days: int = 30, hourly_days: int = 730) -> Plan:
    rows_per_day = nodes * DAY // period_s
    raw_mb = rows_per_day * raw_days * sizes.reading_bytes / 1e6
    hourly_mb_per_year = nodes * 24 * 365 * sizes.hourly_bytes / 1e6
    total_mb = raw_mb + hourly_mb_per_year * hourly_days / 365
    return Plan(nodes, rows_per_day, raw_mb, hourly_mb_per_year, total_mb)


def plan_table(sizes: RowSizes, fleets: tuple[int, ...] = (10, 100, 1000), **kw: int) -> str:
    lines = [
        f"measured: {sizes.reading_bytes:.0f} B per raw reading (with indexes), "
        f"{sizes.hourly_bytes:.0f} B per hourly row",
        "| nodes | rows/day | raw table at steady state | hourly per year | total after retention |",
        "|---|---|---|---|---|",
    ]
    for n in fleets:
        p = plan(n, sizes, **kw)
        lines.append(
            f"| {p.nodes} | {p.rows_per_day:,} | {p.raw_mb:,.1f} MB | {p.hourly_mb_per_year:,.1f} MB"
            f" | {p.total_mb:,.1f} MB |"
        )
    return "\n".join(lines)
