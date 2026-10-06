#!/usr/bin/env python3
"""批处理编排服务调用示例（仅用标准库，无需安装依赖）。

用法：
    python examples/client_demo.py [BASE_URL]

流程演示：
1. 提交一个 4 任务 DAG 工作流（带幂等键）；
2. 重复提交同一幂等键+相同请求，验证返回原工作流（replayed=true）；
3. 同一幂等键换请求体，验证返回 409 冲突；
4. 轮询领取任务 -> 模拟一次失败重试 -> 成功，直到工作流完成；
5. 查询最终状态。
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"


def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict | None]:
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


def claim(workflow_id: str):
    status_code, payload = request(
        "POST", f"/workflows/{workflow_id}/claim", {}
    )
    if status_code == 204:
        return None
    assert status_code == 200, payload
    return payload


def main() -> None:
    idem_key = f"demo-{int(time.time())}"
    workflow_body = {
        "idempotency_key": idem_key,
        "max_attempts": 3,
        "tasks": [
            {"key": "extract", "dependencies": []},
            {"key": "transform", "dependencies": ["extract"]},
            {"key": "validate", "dependencies": ["extract"]},
            {"key": "load", "dependencies": ["transform", "validate"]},
        ],
    }

    print("== 1. 提交工作流 ==")
    code, wf = request("POST", "/workflows", workflow_body)
    print(code, "workflow_id =", wf["id"], "replayed =", wf["replayed"])
    wf_id = wf["id"]

    print("== 2. 相同幂等键+相同请求 -> 原工作流 ==")
    code, again = request("POST", "/workflows", workflow_body)
    print(code, "same id =", again["id"] == wf_id, "replayed =", again["replayed"])

    print("== 3. 相同幂等键+不同请求 -> 409 ==")
    conflicting = json.loads(json.dumps(workflow_body))
    conflicting["tasks"].append({"key": "extra", "dependencies": ["load"]})
    code, err = request("POST", "/workflows", conflicting)
    print(code, err["error"]["code"])

    print("== 4. 工作进程轮询执行 ==")
    failed_once: set[str] = set()
    while True:
        lease = claim(wf_id)
        if lease is None:
            break
        print(
            f"  领取 {lease['key']} 第 {lease['attempt']} 次尝试, "
            f"租约截止 {lease['leased_until']}"
        )

        # 模拟 transform 第一次失败，重试后成功
        if lease["key"] == "transform" and lease["attempt"] == 1:
            code, res = request(
                "POST",
                f"/tasks/{lease['task_id']}/fail",
                {"token": lease["token"], "error": "simulated flaky failure"},
            )
            print("   fail ->", code, res["status"])
            continue

        code, res = request(
            "POST",
            f"/tasks/{lease['task_id']}/succeed",
            {"token": lease["token"], "result": f"{lease['key']} done"},
        )
        print("   succeed ->", code, res["status"])

    print("== 5. 最终状态 ==")
    code, detail = request("GET", f"/workflows/{wf_id}")
    print("workflow status:", detail["status"])
    for task in detail["tasks"]:
        print(
            f"  - {task['key']:<10} {task['status']:<10} "
            f"attempts={task['attempts']} deps={task['dependencies']}"
        )


if __name__ == "__main__":
    main()
