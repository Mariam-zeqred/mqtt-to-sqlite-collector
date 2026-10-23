"""Command line: `python -m collector <run|serve|replay|compact|plan>`."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import tempfile
import threading
import time
from pathlib import Path

from .config import Settings
from .ingest import Ingestor
from .retention import measure_row_sizes, plan_table
from .store import Store, stats


def cmd_run(settings: Settings) -> int:
    """Ingest from the broker and serve the API from one process (API in a thread, uvicorn)."""
    import uvicorn

    from .api import create_app

    store = Store(settings.db_path)
    ingestor = Ingestor(settings, store)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings.db_path), settings.api_host, settings.api_port, log_level="warning"
        )
    )
    api_thread = threading.Thread(target=server.run, daemon=True)
    api_thread.start()
    logging.info("API on http://%s:%d, ingesting %s", settings.api_host, settings.api_port, ingestor.topics)
    ingestor.run_forever(stop)
    server.should_exit = True
    store.close()
    logging.info("stopped: %s", dict(ingestor.counts))
    return 0


def cmd_serve(settings: Settings) -> int:
    import uvicorn

    from .api import create_app

    uvicorn.run(create_app(settings.db_path), host=settings.api_host, port=settings.api_port)
    return 0


def cmd_replay(settings: Settings, path: Path, speed: float) -> int:
    """Feed a JSON-lines file ({"topic":..., "payload":{...}} per line) through the ingest path, no broker.

    Each line is "received" at its payload ts (or now); with --speed > 0 the replay sleeps proportionally.
    """
    store = Store(settings.db_path)
    ingestor = Ingestor(settings, store)
    previous_ts: int | None = None
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            item = json.loads(line)
            payload = item["payload"]
            ts = payload.get("ts") if isinstance(payload, dict) else None
            if speed > 0 and previous_ts is not None and ts is not None:
                time.sleep(max(0, ts - previous_ts) / speed)
            previous_ts = ts if ts is not None else previous_ts
            body = payload if isinstance(payload, str) else json.dumps(payload)
            ingestor.handle(item["topic"], body.encode(), received=ts or int(time.time()))
    print(json.dumps({"outcomes": dict(ingestor.counts), **stats(store.conn)}, indent=2))
    store.close()
    return 0


def cmd_compact(settings: Settings) -> int:
    store = Store(settings.db_path)
    print(json.dumps(store.compact(int(time.time()), settings.raw_days, settings.hourly_days), indent=2))
    store.close()
    return 0


def cmd_plan(settings: Settings, period_s: int) -> int:
    with tempfile.TemporaryDirectory() as tmp:
        sizes = measure_row_sizes(Path(tmp))
    print(
        f"period {period_s} s per node, raw kept {settings.raw_days} d, hourly kept {settings.hourly_days} d"
    )
    print(plan_table(sizes, period_s=period_s, raw_days=settings.raw_days, hourly_days=settings.hourly_days))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="collector", description=__doc__)
    parser.add_argument("--db", type=Path, help="override DB_PATH")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="subscribe to the broker, store, serve the API")
    sub.add_parser("serve", help="serve the API only")
    sub.add_parser("compact", help="roll up and vacuum once")
    p_replay = sub.add_parser("replay", help="ingest a JSON-lines file without a broker")
    p_replay.add_argument("file", type=Path)
    p_replay.add_argument(
        "--speed", type=float, default=0, help="0 = as fast as possible, 60 = 1 min per second"
    )
    p_plan = sub.add_parser("plan", help="measure row sizes and print the retention table")
    p_plan.add_argument("--period", type=int, default=30, help="seconds between readings per node")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    if args.db:
        settings = Settings(**{**settings.__dict__, "db_path": args.db})
    if args.cmd == "run":
        return cmd_run(settings)
    if args.cmd == "serve":
        return cmd_serve(settings)
    if args.cmd == "replay":
        return cmd_replay(settings, args.file, args.speed)
    if args.cmd == "compact":
        return cmd_compact(settings)
    return cmd_plan(settings, args.period)


if __name__ == "__main__":
    sys.exit(main())
