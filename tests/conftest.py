"""pytest 公共夹具：临时 SQLite + TestClient。"""
from __future__ import annotations

import os
import tempfile

import pytest

_tmp = tempfile.TemporaryDirectory()
_db_path = os.path.join(_tmp.name, "test_orch.db")

os.environ.update(
    {
        "ORCH_DB_PATH": _db_path,
        "ORCH_LEASE_SECONDS": "30",
        "ORCH_DEFAULT_MAX_ATTEMPTS": "3",
        "ORCH_REAPER_INTERVAL_SECONDS": "0",  # 测试中关闭后台线程，走懒回收
        "ORCH_BUSY_TIMEOUT_MS": "5000",
    }
)

from fastapi.testclient import TestClient  # noqa: E402

from app.config import load_settings  # noqa: E402
from app.db import connect, init_db, set_settings  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture(scope="session")
def settings():
    s = load_settings()
    set_settings(s)
    return s


@pytest.fixture()
def client(settings):
    if os.path.exists(settings.db_path):
        os.remove(settings.db_path)
    for suffix in ("-wal", "-shm"):
        p = settings.db_path + suffix
        if os.path.exists(p):
            os.remove(p)
    init_db(settings)
    app = create_app()
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def raw_conn(settings):
    conn = connect(settings)
    try:
        yield conn
    finally:
        conn.close()


def make_workflow(tasks, key="idem-1", max_attempts=3):
    return {"tasks": tasks, "idempotency_key": key, "max_attempts": max_attempts}


def t(key, deps=None, max_attempts=None):
    node = {"key": key, "dependencies": deps or []}
    if max_attempts is not None:
        node["max_attempts"] = max_attempts
    return node
