"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "")
    return value.strip() if value.strip() else default


@dataclass(frozen=True)
class Settings:
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_password: str = ""
    mqtt_ca: str = ""  # path to a CA file; empty means plain TCP
    mqtt_prefix: str = "farm"
    db_path: Path = Path("data/telemetry.db")
    raw_days: int = 30
    hourly_days: int = 730
    compact_every_s: int = 3600
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            mqtt_host=_env("MQTT_HOST", "localhost"),
            mqtt_port=int(_env("MQTT_PORT", "1883")),
            mqtt_user=_env("MQTT_USER", ""),
            mqtt_password=_env("MQTT_PASSWORD", ""),
            mqtt_ca=_env("MQTT_CA", ""),
            mqtt_prefix=_env("MQTT_PREFIX", "farm"),
            db_path=Path(_env("DB_PATH", "data/telemetry.db")),
            raw_days=int(_env("RAW_DAYS", "30")),
            hourly_days=int(_env("HOURLY_DAYS", "730")),
            compact_every_s=int(_env("COMPACT_EVERY_S", "3600")),
            api_host=_env("API_HOST", "0.0.0.0"),
            api_port=int(_env("API_PORT", "8000")),
        )
