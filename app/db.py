"""SQLite 连接管理与建表初始化。

设计要点：
- 每个请求/工作单元一个连接（check_same_thread=False 仅供少量跨线程使用）；
- WAL + busy_timeout，写操作串行化（BEGIN IMMEDIATE），保证多工作进程
  并发领取时不会重复分配；
- 状态与租约全部落库，进程重启后依然有效。
"""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator

from .config import Settings, load_settings

_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


def set_settings(settings: Settings) -> None:
    """测试用：替换全局配置。"""
    global _settings
    _settings = settings


def connect(settings: Settings | None = None) -> sqlite3.Connection:
    settings = settings or get_settings()
    directory = os.path.dirname(os.path.abspath(settings.db_path))
    os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(
        settings.db_path,
        timeout=settings.busy_timeout_ms / 1000.0,
        isolation_level=None,  # 手动事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {int(settings.busy_timeout_ms)}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_conn() -> Iterator[sqlite3.Connection]:
    """FastAPI 依赖：每个请求一个连接，请求结束关闭。"""
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash    TEXT NOT NULL,
    status          TEXT NOT NULL,
    default_max_attempts INTEGER NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id           TEXT PRIMARY KEY,
    workflow_id  TEXT NOT NULL REFERENCES workflows(id),
    task_key     TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL,
    token_hash   TEXT,
    leased_until TEXT,
    result       TEXT,
    error        TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    UNIQUE (workflow_id, task_key)
);

CREATE INDEX IF NOT EXISTS idx_tasks_wf_status
    ON tasks (workflow_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_lease
    ON tasks (status, leased_until);

CREATE TABLE IF NOT EXISTS task_dependencies (
    workflow_id   TEXT NOT NULL REFERENCES workflows(id),
    task_id       TEXT NOT NULL REFERENCES tasks(id),
    depends_on_id TEXT NOT NULL REFERENCES tasks(id),
    PRIMARY KEY (task_id, depends_on_id)
);

CREATE INDEX IF NOT EXISTS idx_deps_depends_on
    ON task_dependencies (depends_on_id);
"""


def init_db(settings: Settings | None = None) -> None:
    """幂等建表，启动与 Compose 首次运行时自动初始化。"""
    conn = connect(settings)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
