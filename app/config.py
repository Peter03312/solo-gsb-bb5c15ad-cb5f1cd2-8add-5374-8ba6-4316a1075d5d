"""运行期配置，全部来自环境变量，带默认值。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _as_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    db_path: str
    lease_seconds: int
    default_max_attempts: int
    reaper_interval_seconds: float
    # SQLite 忙等待（毫秒），缓解并发写冲突
    busy_timeout_ms: int


def load_settings() -> Settings:
    return Settings(
        db_path=os.getenv("ORCH_DB_PATH", "/data/orchestrator.db"),
        lease_seconds=_as_int("ORCH_LEASE_SECONDS", 30),
        default_max_attempts=_as_int("ORCH_DEFAULT_MAX_ATTEMPTS", 3),
        reaper_interval_seconds=float(
            os.getenv("ORCH_REAPER_INTERVAL_SECONDS", "10")
        ),
        busy_timeout_ms=_as_int("ORCH_BUSY_TIMEOUT_MS", 5000),
    )
