from pathlib import Path

from collector.retention import RowSizes, measure_row_sizes, plan, plan_table


def test_plan_arithmetic():
    sizes = RowSizes(reading_bytes=100, hourly_bytes=80)
    p = plan(10, sizes, period_s=30, raw_days=30, hourly_days=365)
    assert p.rows_per_day == 10 * 2880
    assert p.raw_mb == 10 * 2880 * 30 * 100 / 1e6
    assert p.hourly_mb_per_year == 10 * 24 * 365 * 80 / 1e6
    assert p.total_mb == p.raw_mb + p.hourly_mb_per_year
    assert plan(1000, sizes).rows_per_day == 100 * plan(10, sizes).rows_per_day


def test_measured_sizes_are_plausible(tmp_path: Path):
    sizes = measure_row_sizes(tmp_path, n=3000)
    assert 40 < sizes.reading_bytes < 300  # 9 columns + two indexes; SQLite pages are 4 KiB
    assert 20 < sizes.hourly_bytes < 300
    table = plan_table(sizes)
    assert table.count("\n") == 5 and "| 1000 |" in table
