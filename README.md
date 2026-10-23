# mqtt-to-sqlite-collector

A small Python service that subscribes to the MQTT topics published by
[esp32-mqtt-sensor-node](https://github.com/Mariam-zeqred/esp32-mqtt-sensor-node), validates every message with
pydantic, stores it in SQLite (WAL mode), rolls old raw rows up into hourly aggregates, and serves the data over a
tiny FastAPI read API. A Docker Compose file adds Mosquitto and Grafana with a provisioned dashboard. The service
itself is about 600 lines of Python (765 with comments; tests and tools are extra) and runs on a Raspberry Pi or a
small VPS; no time-series database is needed for a farm's worth of nodes.

```
 ESP32 nodes                 broker                    collector (one process)                      readers
┌──────────────┐   TLS/TCP  ┌───────────┐  subscribe  ┌──────────────┬───────────┬──────────────┐   ┌────────────┐
│ SHT31 → JSON │ ─────────► │ Mosquitto │ ──────────► │ paho client  │ pydantic  │ SQLite (WAL) │◄──│ FastAPI    │ ◄── curl / app
│ every 30 s   │  QoS 1     │           │ farm/+/…    │ (backoff)    │ validate  │ readings     │   │ read-only  │
└──────────────┘            └───────────┘             │              │ reject    │ hourly       │◄──│ Grafana    │ ◄── browser
                                                      │ every hour:  │           │ status       │   │ (sqlite    │
                                                      │ roll-up +    │           │ rejected     │   │  plugin)   │
                                                      │ vacuum       │           │              │   └────────────┘
                                                      └──────────────┴───────────┴──────────────┘
```

Topics and documents are exactly the node firmware's: `farm/<node>/telemetry` carries
`{"node","ts","seq","temp_c","rh_pct","rssi_dbm","uptime_s","fw"}` (`null` for an unsynced clock or a sensor
fault), `farm/<node>/status` carries `{"node","state","ts"}` with `online` / `sleeping` / `offline` (last will).

## Run it

Without a broker, in four commands (this is what `make check` does):

```bash
pip install -r requirements-dev.txt
make check                                   # ruff + 35 tests + replay the sample + compact + retention table
python -m collector --db demo.db replay data/sample_incubator.jsonl      # 1 367 real readings, see below
DB_PATH=demo.db python -m collector serve    # then: curl localhost:8000/nodes
```

With a broker and real or fake nodes:

```bash
cp .env.example .env                         # broker host, credentials, retention; never committed
python -m collector run                      # ingest + API in one process, compacts every COMPACT_EVERY_S
python tools/publish_fake_node.py --count 60 # a SYNTHETIC node, or --replay data/sample_incubator.jsonl
```

The full stack: `docker compose up -d` starts Mosquitto (1883, password file in `docker/passwd`, see
`docker/mosquitto.conf` for the TLS block that matches the node repo), the collector (API on 8000) and Grafana
(3000) with the SQLite datasource plugin and `grafana/dashboard.json` provisioned. The sandbox this was built in
has no Docker daemon, so the Compose stack was **not run here**; CI runs `docker compose config` and builds the
image, and every SQL query in the dashboard is executed against the real schema by `tests/test_grafana.py`.

## What comes out

Real responses from `python -m collector serve` on the replayed sample (the `hourly` call was made after
`RAW_DAYS=7 python -m collector compact` ran in a second process while the server kept answering, which is what
WAL mode buys):

```
GET /nodes
[{"node":"hive-1C5A00","readings":1367,"last_received":1791042283,"state":"online"}]

GET /nodes/hive-1C5A00/latest
{"id":1367,"node":"hive-1C5A00","ts":1791042283,"received":1791042283,"seq":1366,"temp_c":38.42,"rh_pct":67.1,
 "rssi_dbm":null,"uptime_s":null,"fw":null}

GET /nodes/hive-1C5A00/hourly?since=0&until=9999999999&limit=1
[{"node":"hive-1C5A00","hour":1791021600,"n":233,"temp_min":26.83,"temp_mean":31.73,"temp_max":33.42,
  "rh_min":70.1,"rh_mean":74.96,"rh_max":81.5}]

GET /nodes/nope/latest            -> 404 {"detail":"no readings for node 'nope'"}
GET /nodes/x/readings?limit=0     -> 422 (limit must be 1..10000)
```

| Endpoint | Returns |
|---|---|
| `GET /health` | row count, age of the newest reading in seconds (alert on this) |
| `GET /nodes` | every node with its reading count, last `received` and last status |
| `GET /nodes/{node}/latest` | newest raw row |
| `GET /nodes/{node}/readings?since&until&limit` | raw rows, default the last 24 h, max 10 000 |
| `GET /nodes/{node}/hourly?since&until&limit` | hourly min / mean / max, default the last 30 days |
| `GET /rejected?limit` | newest rejected messages with the validation reason |
| `GET /stats` | row counts per table and the database size in bytes |

**The sample data is real.** `data/sample_incubator.jsonl` is the 3 October 2026 history of Mariam's queen-bee
incubator (`hive-1C5A00`, 1 367 readings at 15 s) copied from
[incubator-telemetry-analysis](https://github.com/Mariam-zeqred/incubator-telemetry-analysis) and rewritten into
the node's topic/JSON format by `tools/make_sample.py`. Only the logged fields are present (`ts`, `temp_c`,
`rh_pct`, `seq` = index); `rssi_dbm`, `uptime_s` and `fw` were not logged by that device and are left `null`
rather than invented. `tools/publish_fake_node.py` without `--replay` produces clearly labelled synthetic values.

## Validation and storage

`parse_message` never raises: a message is a `Telemetry`, a `Status` or a `Rejected` with a one-line reason
(`temp_c: Input should be less than or equal to 125`, `node in payload != node in topic`, `not JSON`, ...).
Ranges follow the SHT31 datasheet (−40..125 °C, 0..100 %); `ts` must be between 2017 and 2096 or `null`;
unknown keys are ignored so a newer firmware does not break ingest. Rejected messages are stored for a week so
a misbehaving node is debuggable from `/rejected`.

SQLite is opened in WAL mode with `synchronous=NORMAL`: the paho network thread inserts while the API opens a
read-only connection per request and Grafana reads the file directly. QoS 1 redelivery is dropped by a unique
index on `(node, ts, seq)` (only when `ts` is known; without a clock there is no identity, so nothing is dropped).

## Retention

Every `COMPACT_EVERY_S` (default one hour) the collector rolls every raw reading older than `RAW_DAYS` into
`hourly(node, hour, n, temp_min/mean/max, rh_min/mean/max)`, deletes those raw rows, drops `hourly` rows older
than `HOURLY_DAYS` and `rejected` rows older than 7 days, then runs an incremental vacuum and a WAL checkpoint so
the file actually shrinks. The cutoff is aligned to the hour, so a bucket is rolled up only once it is complete,
and a bucket that receives late rows is merged (weighted mean, min/max). Retention is measured on `received`
(the collector's clock), because `ts` can be `null`.

How big does the file get? `python -m collector plan` inserts 20 000 realistic rows into a temporary database
and measures the real bytes per row (SQLite 3.45.1, 4 KiB pages), then does the arithmetic. Output from this
repo's `make check`:

```
measured: 126 B per raw reading (with indexes), 110 B per hourly row
```

| nodes | rows/day (30 s period) | raw table at steady state (30 d) | hourly per year | total after 2 years |
|---|---|---|---|---|
| 10 | 28 800 | 109 MB | 9.7 MB | 128 MB |
| 100 | 288 000 | 1.09 GB | 97 MB | 1.28 GB |
| 1 000 | 2 880 000 | 10.9 GB | 0.97 GB | 12.8 GB |

So one node at 30 s costs about 11 MB of raw history per month and under 1 MB per year of hourly history. At
1 000 nodes the write rate (33 inserts/s) is still trivial for SQLite, but the single file is 10 GB and a hostile
`readings` query can hold a page cache for seconds; that is the point where Timescale/InfluxDB or partitioning by
month become worth their complexity.

## Tests (`make check`, also in CI on Python 3.11 and 3.12)

| What | How |
|---|---|
| Message schemas | 10 tests: the firmware's exact document, nulls, NaN, ranges, topic/payload mismatch, prefix, status |
| Store | 10 tests: WAL + incremental vacuum pragmas, duplicate detection, roll-up maths vs `statistics.mean`, hour-aligned cutoff, late-row merge, expiry, read-only reader, the file really shrinks |
| API | 5 tests through Starlette's TestClient |
| Ingest | 5 tests with a fake paho client and a closed store (the network thread must never die) |
| End to end | an in-process [amqtt](https://github.com/Yakifo/amqtt) broker, the collector's own paho client, a paho "node": 6 stored, 1 duplicate, 2 rejected |
| Retention, CLI, Grafana | plan arithmetic and measured sizes, `replay` → `compact` → `plan`, all 8 dashboard queries run on the schema |

## Limitations

- One writer process. Two collectors on the same file would fight for the write lock; run one per database.
- `synchronous=NORMAL` can lose the last transaction on power loss (not on a crash). Set `FULL` if that matters.
- Hourly roll-up keeps min/mean/max only; percentiles or heater duty need the raw rows (keep `RAW_DAYS` longer).
- The Compose stack and the Grafana dashboard were not started in the build environment (no Docker daemon).
  The compose file is validated and the image built in CI; the dashboard JSON follows the
  frser-sqlite-datasource query model and its SQL is tested, but it has not been looked at in a browser yet.
- No authentication on the API; put it behind a reverse proxy or bind it to localhost (`API_HOST=127.0.0.1`).

## Next steps

- Daily roll-up table on top of hourly; `GET /nodes/{node}/daily`.
- Alert rule in Grafana on `last_reading_age_s` (node silent) and on `rejected` > 0.
- A `/export.csv` endpoint and a `tools/backup.sh` using `sqlite3 .backup` (safe while writing, thanks to WAL).

## Repository map

```
src/collector/models.py     pydantic Telemetry / Status / Rejected, parse_message, topic_parts
src/collector/store.py      schema, Store (writer), compact(), read helpers, open_reader()
src/collector/ingest.py     Ingestor: paho wiring, handle(), compact(), run_forever()
src/collector/api.py        create_app(db_path) – FastAPI, read-only connection per request
src/collector/retention.py  measure_row_sizes(), plan(), plan_table()
src/collector/__main__.py   run | serve | replay | compact | plan
src/collector/config.py     Settings.from_env()
tests/                      35 tests (see the table above)
tools/                      make_sample.py, publish_fake_node.py, make_dashboard.py
data/sample_incubator.jsonl 1 367 real readings + 1 status message (190 kB)
docker-compose.yml, Dockerfile, docker/mosquitto.conf, grafana/ (provisioning + dashboard.json)
```

Licence: MIT.
