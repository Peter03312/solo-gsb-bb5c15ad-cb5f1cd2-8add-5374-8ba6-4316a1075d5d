"""领取、上报、重试、上限、级联跳过、取消、令牌鉴权。"""
from tests.conftest import make_workflow, t


def submit(client, tasks, **kw):
    body = make_workflow(tasks, key=kw.pop("key", "wf"), **kw)
    return client.post("/workflows", json=body).json()


def claim(client, wf_id):
    resp = client.post(f"/workflows/{wf_id}/claim", json={})
    if resp.status_code == 204:
        return None
    assert resp.status_code == 200, resp.text
    return resp.json()


def succeed(client, lease, result=None):
    return client.post(
        f"/tasks/{lease['task_id']}/succeed",
        json={"token": lease["token"], "result": result},
    )


def fail(client, lease, error="boom"):
    return client.post(
        f"/tasks/{lease['task_id']}/fail",
        json={"token": lease["token"], "error": error},
    )


def status_map(wf):
    return {x["key"]: x["status"] for x in wf["tasks"]}


def test_claim_respects_dependencies_and_succeeds_workflow(client):
    wf = submit(client, [t("a"), t("b", ["a"]), t("c", ["b"])])
    wf_id = wf["id"]

    # 最初只有 a 可领取
    first = claim(client, wf_id)
    assert first["key"] == "a"
    assert claim(client, wf_id) is None
    assert first["attempt"] == 1

    resp = succeed(client, first, result="done-a")
    assert resp.status_code == 200

    second = claim(client, wf_id)
    assert second["key"] == "b"
    succeed(client, second)

    third = claim(client, wf_id)
    assert third["key"] == "c"
    succeed(client, third)

    assert claim(client, wf_id) is None

    detail = client.get(f"/workflows/{wf_id}").json()
    assert detail["status"] == "succeeded"
    assert set(status_map(detail).values()) == {"succeeded"}


def test_only_valid_token_can_report(client):
    wf = submit(client, [t("a")])
    lease = claim(client, wf["id"])

    resp = client.post(
        f"/tasks/{lease['task_id']}/succeed", json={"token": "wrong-token"}
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "invalid_token"

    assert succeed(client, lease).status_code == 200


def test_token_single_use_old_token_rejected(client):
    wf = submit(client, [t("a")], max_attempts=3)
    lease1 = claim(client, wf["id"])
    fail(client, lease1)  # 回到 pending
    lease2 = claim(client, wf["id"])
    assert lease2["attempt"] == 2
    assert lease2["token"] != lease1["token"]

    # 旧令牌失效
    resp = succeed(client, lease1)
    assert resp.status_code == 403
    assert succeed(client, lease2).status_code == 200


def test_retries_then_failure_skips_descendants_other_branch_runs(client):
    wf = submit(
        client,
        [
            t("x"),
            t("a", ["x"], max_attempts=2),
            t("b", ["a"]),
            t("c", ["b"]),
            t("y"),
            t("z", ["y"]),
        ],
        key="branches",
        max_attempts=5,
    )
    wf_id = wf["id"]

    # 根任务 x 与 y 均可领取，按 key 顺序先 x 后 y
    lx = claim(client, wf_id)
    assert lx["key"] == "x"
    succeed(client, lx)

    la = claim(client, wf_id)
    assert la["key"] == "a"
    fail(client, la)  # 第 1 次失败，可重领
    la2 = claim(client, wf_id)
    assert la2["attempt"] == 2
    fail(client, la2, error="permanent")  # 达到上限 -> failed

    # a 链：b、c 被级联跳过；另一分支 y、z 继续
    ly = claim(client, wf_id)
    assert ly["key"] == "y"
    succeed(client, ly)
    lz = claim(client, wf_id)
    assert lz["key"] == "z"
    succeed(client, lz)
    assert claim(client, wf_id) is None

    detail = client.get(f"/workflows/{wf_id}").json()
    assert detail["status"] == "failed"
    smap = status_map(detail)
    assert smap == {
        "x": "succeeded",
        "a": "failed",
        "b": "skipped",
        "c": "skipped",
        "y": "succeeded",
        "z": "succeeded",
    }
    a_task = next(t_ for t_ in detail["tasks"] if t_["key"] == "a")
    assert a_task["attempts"] == 2
    assert a_task["error"] == "permanent"


def test_lease_expiry_makes_task_reclaimable(client):
    from datetime import datetime, timezone

    wf = submit(client, [t("a", max_attempts=2)], key="lease")
    lease = claim(client, wf["id"])
    wf_id = wf["id"]

    # 手动把租约改到过去，模拟到期
    past = "2000-01-01T00:00:00+00:00"
    from app.db import get_settings
    from app.service import transaction

    settings = get_settings()
    from app.db import connect

    conn = connect(settings)
    with transaction(conn):
        conn.execute(
            "UPDATE tasks SET leased_until = ? WHERE id = ?",
            (past, lease["task_id"]),
        )
    conn.close()

    # 查询触发懒回收，任务回到 pending 可重领
    detail = client.get(f"/workflows/{wf_id}").json()
    assert status_map(detail)["a"] == "pending"

    again = claim(client, wf_id)
    assert again["attempt"] == 2

    # 到期前的旧令牌拒绝
    resp = client.post(
        f"/tasks/{lease['task_id']}/succeed", json={"token": lease["token"]}
    )
    assert resp.status_code == 403


def test_lease_expiry_at_max_attempts_fails(client):
    wf = submit(client, [t("a", max_attempts=1)], key="lease-max")
    lease = claim(client, wf_id=wf["id"])
    assert lease["attempt"] == 1

    from app.db import connect, get_settings
    from app.service import transaction

    conn = connect(get_settings())
    with transaction(conn):
        conn.execute(
            "UPDATE tasks SET leased_until = ? WHERE id = ?",
            ("2000-01-01T00:00:00+00:00", lease["task_id"]),
        )
    conn.close()

    detail = client.get(f"/workflows/{wf['id']}").json()
    assert status_map(detail)["a"] == "failed"
    assert detail["status"] == "failed"
    assert claim(client, wf["id"]) is None


def test_cancel_cancels_unfinished_and_blocks_claims_and_tokens(client):
    wf = submit(
        client, [t("a"), t("b", ["a"]), t("c", ["a"])], key="cancel-1"
    )
    wf_id = wf["id"]
    lease = claim(client, wf_id)
    succeed(client, lease)
    lb = claim(client, wf_id)
    assert lb["key"] == "b"

    resp = client.post(f"/workflows/{wf_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    # b 已租约、c 仍 pending，都转为取消
    assert resp.json()["cancelled_tasks"] == 2

    # 禁止领取
    assert claim(client, wf_id) is None
    # 旧令牌拒绝
    assert succeed(client, lb).status_code == 403

    detail = client.get(f"/workflows/{wf_id}").json()
    assert detail["status"] == "cancelled"
    assert status_map(detail) == {
        "a": "succeeded",
        "b": "cancelled",
        "c": "cancelled",
    }

    # 已取消工作流重复取消为幂等成功
    again = client.post(f"/workflows/{wf_id}/cancel")
    assert again.status_code == 200
    assert again.json()["cancelled_tasks"] == 0


def test_global_claim_across_workflows(client):
    submit(client, [t("a")], key="g1")
    submit(client, [t("p")], key="g2")
    got = []
    for _ in range(2):
        resp = client.post("/claims/claim", json={})
        assert resp.status_code == 200
        got.append(resp.json()["key"])
    assert sorted(got) == ["a", "p"]
    assert client.post("/claims/claim", json={}).status_code == 204


def test_unknown_workflow_and_task_404(client):
    assert client.get("/workflows/nope").status_code == 404
    assert client.post("/workflows/nope/cancel").status_code == 404
    resp = client.post(
        "/tasks/nope/succeed", json={"token": "t"}
    )
    assert resp.status_code == 404
