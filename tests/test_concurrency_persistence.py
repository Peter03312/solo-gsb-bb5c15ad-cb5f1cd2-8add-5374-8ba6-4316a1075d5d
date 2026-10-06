"""并发领取不重复分配；重启后状态与租约保持。"""
import os

from app.db import connect, get_settings
from app import service
from tests.conftest import make_workflow, t


def _submit_raw(conn, settings, tasks, key="conc"):
    from app.models import WorkflowIn

    payload = WorkflowIn.model_validate(
        make_workflow(tasks, key=key, max_attempts=3)
    )
    detail, _ = service.create_workflow(conn, payload, settings)
    return detail["id"]


def test_concurrent_claims_never_double_assign(settings):
    """20 个工作进程线程并发领取，只能有 10 个成功且任务互不重复。"""
    import threading

    conn0 = connect(settings)
    try:
        wf_id = _submit_raw(
            conn0, settings, [t(f"task-{i}") for i in range(10)]
        )
    finally:
        conn0.close()

    results: list[str | None] = []
    lock = threading.Lock()

    def worker():
        conn = connect(settings)
        try:
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            claimed = service.claim_task(
                conn, settings, now, workflow_id=wf_id
            )
        finally:
            conn.close()
        with lock:
            results.append(claimed["task_id"] if claimed else None)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    won = [r for r in results if r is not None]
    assert len(won) == 10
    assert len(set(won)) == 10


def test_state_and_lease_survive_restart(settings, client):
    """模拟重启：新建连接/新 TestClient 后，状态、尝试计数、租约与令牌仍有效。"""
    from fastapi.testclient import TestClient

    from app.main import create_app

    body = make_workflow([t("a"), t("b", ["a"])], key="restart-1")
    wf = client.post("/workflows", json=body).json()
    wf_id = wf["id"]
    lease = client.post(
        f"/workflows/{wf_id}/claim", json={}
    ).json()

    # —— 重启进程：丢掉 TestClient，重新 create_app ——
    app2 = create_app()
    with TestClient(app2) as c2:
        detail = c2.get(f"/workflows/{wf_id}").json()
        assert detail["status"] == "running"
        tasks = {x["key"]: x for x in detail["tasks"]}
        assert tasks["a"]["status"] == "leased"
        assert tasks["a"]["leased_until"] == lease["leased_until"]

        # 租约未到期：不能重复领取 a
        assert (
            c2.post(f"/workflows/{wf_id}/claim", json={}).status_code == 204
        )
        # 同一令牌在重启后仍可成功上报
        resp = c2.post(
            f"/tasks/{lease['task_id']}/succeed",
            json={"token": lease["token"], "result": "ok"},
        )
        assert resp.status_code == 200

        # 再次重启，b 依赖已满足，可以领取
        app3 = create_app()
        with TestClient(app3) as c3:
            lease_b = c3.post(
                f"/workflows/{wf_id}/claim", json={}
            ).json()
            assert lease_b["key"] == "b"
            c3.post(
                f"/tasks/{lease_b['task_id']}/succeed",
                json={"token": lease_b["token"]},
            )
            final = c3.get(f"/workflows/{wf_id}").json()
            assert final["status"] == "succeeded"


def test_db_file_persists_on_disk(settings):
    assert os.path.exists(settings.db_path)
