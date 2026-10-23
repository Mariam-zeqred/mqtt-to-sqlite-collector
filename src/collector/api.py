"""Read-only HTTP API over the SQLite file. Each request opens its own read-only connection (WAL-safe)."""

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query

from . import __version__
from . import store as q

MAX_ROWS = 10_000
DAY = 86_400


def create_app(db_path: Path | str) -> FastAPI:
    db_path = Path(db_path)
    app = FastAPI(title="mqtt-to-sqlite-collector", version=__version__)

    def conn_dep() -> Iterator[Any]:
        if not db_path.exists():
            raise HTTPException(503, f"database {db_path} does not exist yet")
        c = q.open_reader(db_path)
        try:
            yield c
        finally:
            c.close()

    DB = Annotated[Any, Depends(conn_dep)]
    Limit = Annotated[int, Query(ge=1, le=MAX_ROWS)]

    def window(since: int | None, until: int | None, default_span: int) -> tuple[int, int]:
        until_ = until if until is not None else int(time.time()) + 1
        return (since if since is not None else until_ - default_span, until_)

    @app.get("/health")
    def health(c: DB) -> dict[str, Any]:
        s = q.stats(c)
        age = None if s["last_received"] is None else int(time.time()) - s["last_received"]
        return {
            "status": "ok",
            "db": str(db_path),
            "readings": s["rows"]["readings"],
            "last_reading_age_s": age,
        }

    @app.get("/nodes")
    def nodes(c: DB) -> list[dict[str, Any]]:
        return q.nodes(c)

    @app.get("/nodes/{node}/latest")
    def latest(c: DB, node: str) -> dict[str, Any]:
        row = q.latest(c, node)
        if row is None:
            raise HTTPException(404, f"no readings for node {node!r}")
        return row

    @app.get("/nodes/{node}/readings")
    def readings(
        c: DB, node: str, since: int | None = None, until: int | None = None, limit: Limit = 1000
    ) -> list[dict[str, Any]]:
        """Raw readings, newest window first by default: the last 24 h, ordered by `received`."""
        return q.readings(c, node, *window(since, until, DAY), limit)

    @app.get("/nodes/{node}/hourly")
    def hourly(
        c: DB, node: str, since: int | None = None, until: int | None = None, limit: Limit = 1000
    ) -> list[dict[str, Any]]:
        """Hourly min/mean/max, default window the last 30 days."""
        return q.hourly(c, node, *window(since, until, 30 * DAY), limit)

    @app.get("/rejected")
    def rejected(c: DB, limit: Annotated[int, Query(ge=1, le=1000)] = 50) -> list[dict[str, Any]]:
        return q.rejected(c, limit)

    @app.get("/stats")
    def stats(c: DB) -> dict[str, Any]:
        return q.stats(c)

    return app
