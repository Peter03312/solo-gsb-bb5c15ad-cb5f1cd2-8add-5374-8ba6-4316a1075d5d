"""SQLite persistence layer.

A single connection guarded by an RLock: SQLite serializes writers anyway,
and BEGIN IMMEDIATE transactions give us atomic read-modify-write for the
claim path, so concurrent workers can never be assigned the same task.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    workflow_id     TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash    TEXT NOT NULL,
    status          TEXT NOT NULL,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    workflow_id      TEXT NOT NULL,
    task_id          TEXT NOT NULL,
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    lease_token      TEXT,
    lease_expires_at REAL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    PRIMARY KEY (workflow_id, task_id),
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
);

CREATE TABLE IF NOT EXISTS task_deps (
    workflow_id TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    depends_on  TEXT NOT NULL,
    PRIMARY KEY (workflow_id, task_id, depends_on),
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_claimable
    ON tasks(workflow_id, status, lease_expires_at);
"""


class Database:
    def __init__(self, path: str):
        if path != ":memory:":
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Exclusive write transaction (BEGIN IMMEDIATE ... COMMIT/ROLLBACK)."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
