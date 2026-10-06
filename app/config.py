"""Runtime configuration, read from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_path: str
    default_lease_seconds: int
    max_lease_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_path=os.environ.get("DATABASE_PATH", "./data/orchestrator.db"),
            default_lease_seconds=int(os.environ.get("DEFAULT_LEASE_SECONDS", "30")),
            max_lease_seconds=int(os.environ.get("MAX_LEASE_SECONDS", "3600")),
        )
