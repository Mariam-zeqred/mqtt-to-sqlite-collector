"""Message schemas (the esp32-mqtt-sensor-node JSON format) and topic parsing.

Telemetry: {"node":"esp32-a1b2c3","ts":1791676800,"seq":42,"temp_c":36.52,"rh_pct":61.25,
            "rssi_dbm":-67,"uptime_s":1234,"fw":"0.1.0"}      on <prefix>/<node>/telemetry
Status:    {"node":"esp32-a1b2c3","state":"online","ts":1791676800}  on <prefix>/<node>/status
The firmware sends null for ts (clock not synced) and for temp_c/rh_pct (sensor fault).
"""

from __future__ import annotations

import json
import math
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

NODE_PATTERN = r"^[A-Za-z0-9_-]{1,64}$"


class Telemetry(BaseModel):
    """One sensor reading. Unknown keys are ignored so a newer firmware does not break ingest."""

    model_config = ConfigDict(extra="ignore", strict=False)

    node: str = Field(pattern=NODE_PATTERN)
    ts: int | None = Field(default=None, ge=1_500_000_000, le=4_000_000_000)  # 2017..2096
    seq: int = Field(ge=0, le=2**32 - 1)
    temp_c: float | None = Field(default=None, ge=-40, le=125)  # SHT31 operating range
    rh_pct: float | None = Field(default=None, ge=0, le=100)
    rssi_dbm: int | None = Field(default=None, ge=-120, le=0)
    uptime_s: int | None = Field(default=None, ge=0)
    fw: str | None = Field(default=None, max_length=32)

    @field_validator("temp_c", "rh_pct", mode="before")
    @classmethod
    def _nan_is_null(cls, v: object) -> object:
        # json.loads accepts NaN/Infinity; treat them like the firmware's null.
        if isinstance(v, float) and not math.isfinite(v):
            return None
        return v


class Status(BaseModel):
    model_config = ConfigDict(extra="ignore")

    node: str = Field(pattern=NODE_PATTERN)
    state: Literal["online", "sleeping", "offline"]
    ts: int | None = Field(default=None, ge=1_500_000_000, le=4_000_000_000)


class Rejected(BaseModel):
    """Why a message was not stored; kept in the `rejected` table for debugging."""

    topic: str
    reason: str
    payload: str


def topic_parts(topic: str, prefix: str) -> tuple[str, str] | None:
    """'farm/esp32-a1/telemetry' -> ('esp32-a1', 'telemetry'); None if the topic is not ours."""
    parts = topic.split("/")
    if len(parts) != 3 or parts[0] != prefix or parts[2] not in ("telemetry", "status") or not parts[1]:
        return None
    return parts[1], parts[2]


def parse_message(topic: str, payload: bytes, prefix: str = "farm") -> Telemetry | Status | Rejected:
    """Validate one MQTT message. Never raises: anything wrong becomes a Rejected."""
    text = payload.decode("utf-8", errors="replace")
    if len(payload) > 1024:
        return Rejected(topic=topic, reason="payload over 1024 bytes", payload=text[:200])
    parts = topic_parts(topic, prefix)
    if parts is None:
        return Rejected(topic=topic, reason="topic not <prefix>/<node>/(telemetry|status)", payload=text)
    node, leaf = parts
    try:
        doc = json.loads(text)
    except ValueError:
        return Rejected(topic=topic, reason="not JSON", payload=text)
    if not isinstance(doc, dict):
        return Rejected(topic=topic, reason="JSON is not an object", payload=text)
    model: type[Telemetry] | type[Status] = Telemetry if leaf == "telemetry" else Status
    try:
        msg = model.model_validate(doc)
    except ValidationError as exc:
        first = exc.errors()[0]
        where = ".".join(str(p) for p in first["loc"]) or "document"
        return Rejected(topic=topic, reason=f"{where}: {first['msg']}", payload=text)
    if msg.node != node:
        return Rejected(
            topic=topic, reason=f"node in payload ({msg.node}) != node in topic ({node})", payload=text
        )
    return msg
