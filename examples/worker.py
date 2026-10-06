#!/usr/bin/env python3
"""通用工作进程：跨所有工作流轮询领取就绪任务并执行。

仅用标准库。任务的“实际执行”用可插拔的 handler 表示；这里以一个
会随机失败的演示 handler 展示重试语义。
"""
from __future__ import annotations

import json
import os
import random
import time
import urllib.error
import urllib.request

BASE_URL = os.getenv("ORCH_API_URL", "http://127.0.0.1:8000")
POLL_INTERVAL = float(os.getenv("WORKER_POLL_INTERVAL", "1"))


def call(method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def execute(key: str, attempt: int) -> None:
    """抛出异常即视为任务失败。按需替换为真实业务逻辑。"""
    if key == "flaky" and attempt == 1 and random.random() < 0.8:
        raise RuntimeError("transient error, please retry")


def main() -> None:
    while True:
        code, lease = call("POST", "/claims/claim", {"worker_id": "demo-worker"})
        if code == 204:
            time.sleep(POLL_INTERVAL)
            continue
        if code != 200:
            print("claim error:", code, lease)
            time.sleep(POLL_INTERVAL)
            continue

        print(f"claimed {lease['key']} attempt={lease['attempt']}")
        try:
            execute(lease["key"], lease["attempt"])
        except Exception as exc:  # noqa: BLE001
            call(
                "POST",
                f"/tasks/{lease['task_id']}/fail",
                {"token": lease["token"], "error": str(exc)},
            )
        else:
            call(
                "POST",
                f"/tasks/{lease['task_id']}/succeed",
                {"token": lease["token"], "result": "ok"},
            )


if __name__ == "__main__":
    main()
