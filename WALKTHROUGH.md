# Walkthrough (for Mariam)

## Why it is designed this way

Your sensor node publishes a JSON document every 30 seconds. Something has to receive it, refuse the broken
ones, keep the rest, throw old rows away on purpose rather than by accident, and answer "what is the temperature
in hive 3 right now?" without a database shell. This repo is the smallest honest version of that: one Python
process, one SQLite file, one HTTP API, and a Grafana dashboard reading the same file.

SQLite instead of Postgres or InfluxDB because, at farm scale, the write rate is tiny (1 000 nodes at 30 s is
33 inserts per second) and a file you can copy with `cp` is worth a lot. WAL mode is what lets the API and
Grafana read while the collector writes. Retention (raw for 30 days, hourly for two years) is the trade every
time-series system makes; here it is 20 lines of SQL you can read.

Validation is strict about ranges and loose about unknown keys: a `temp_c` of 300 is a wiring fault that would
poison every mean, while the next firmware will add a field and the collector must keep ingesting.

## File map

- `src/collector/models.py` is the contract with the firmware. `parse_message` turns bytes into a `Telemetry`,
  a `Status` or a `Rejected` with a reason; it never raises.
- `src/collector/store.py` owns the schema and the single writer connection. `compact()` is the roll-up:
  `INSERT ... SELECT ... GROUP BY hour ... ON CONFLICT DO UPDATE`, deletes, then an incremental vacuum.
- `src/collector/ingest.py` wires paho-mqtt: subscribe on every (re)connect, `handle()` per message, count the
  outcomes. paho's `reconnect_delay_set(1, 60)` is the exponential backoff; a lock separates the network
  thread's inserts from the hourly `compact()`.
- `src/collector/api.py` builds the FastAPI app with a read-only connection per request.
- `src/collector/retention.py` measures bytes per row by inserting rows, then does the fleet arithmetic.
- `tests/test_broker_e2e.py` is the one to show people: a real MQTT broker (amqtt) runs inside the test
  process, the collector connects with its production code, a fake node publishes good, duplicate and broken
  messages.

## Three questions a professor may ask

**1. Why SQLite and not a time-series database?** The numbers say so. Measured here, a raw reading costs
126 bytes with its indexes, so 100 nodes at 30 s produce 1.1 GB per month and 3 writes per second; hourly
history costs under 1 MB per node-year. A Raspberry Pi does that with one file to back up. The honest limit is
around 1 000 nodes, where the raw table is 10 GB and a careless query hurts; then Timescale or partitioning
earns its complexity.

**2. What happens when the broker redelivers a message, or a node's clock is wrong?** QoS 1 means "at least
once", so duplicates are normal; a unique index on `(node, ts, seq)` drops them and a counter records it. A node
without NTP sends `ts: null`; then there is no identity and the row is kept, because dropping real data is worse
than a duplicate. Retention uses the collector's `received` time for the same reason.

**3. How do you know the roll-up is correct?** `tests/test_store.py` builds two hours of readings with a known
pattern and checks `n`, min, max and the mean against `statistics.mean`; a second test shows half-filled buckets
wait for the hour boundary; a third rolls the same hour up twice (late rows after an outage) and checks the
weighted merge. The file-shrink test caught a real bug: Python's `execute("PRAGMA incremental_vacuum")` frees
one page and stops, so `executescript` is used.

## One thing you could extend

Add a `daily` table and `GET /nodes/{node}/daily` with the same `ROLLUP` pattern over `hourly` (weighted by
`n`), then a Grafana alert on `last_reading_age_s`. One evening of work, and the collector becomes the
monitoring back end your poultry-house controller does not have yet.
