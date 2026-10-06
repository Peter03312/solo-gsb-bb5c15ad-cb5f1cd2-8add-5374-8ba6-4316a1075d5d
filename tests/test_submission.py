"""图校验与幂等提交。"""
from tests.conftest import make_workflow, t


def test_submit_simple_graph(client):
    body = make_workflow(
        [t("a"), t("b", ["a"]), t("c", ["a"]), t("d", ["b", "c"])],
        key="wf-1",
    )
    resp = client.post("/workflows", json=body)
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "pending"
    assert data["replayed"] is False
    assert {x["key"] for x in data["tasks"]} == {"a", "b", "c", "d"}
    deps = {x["key"]: x["dependencies"] for x in data["tasks"]}
    assert deps["b"] == ["a"]
    assert sorted(deps["d"]) == ["b", "c"]


def test_missing_dependency_rejected(client):
    body = make_workflow([t("a", ["ghost"])], key="wf-missing")
    resp = client.post("/workflows", json=body)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "missing_dependency"


def test_duplicate_key_rejected(client):
    body = make_workflow([t("a"), t("a")], key="wf-dup")
    resp = client.post("/workflows", json=body)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "duplicate_task_key"


def test_self_dependency_is_cycle(client):
    body = make_workflow([t("a", ["a"])], key="wf-self")
    resp = client.post("/workflows", json=body)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "dependency_cycle"


def test_cycle_rejected(client):
    body = make_workflow(
        [t("a", ["c"]), t("b", ["a"]), t("c", ["b"])], key="wf-cycle"
    )
    resp = client.post("/workflows", json=body)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "dependency_cycle"


def test_idempotency_same_request_returns_original(client):
    body = make_workflow([t("a")], key="idem-same")
    first = client.post("/workflows", json=body).json()
    second = client.post("/workflows", json=body).json()
    assert second["replayed"] is True
    assert second["id"] == first["id"]


def test_idempotency_different_request_conflicts(client):
    body1 = make_workflow([t("a")], key="idem-x")
    client.post("/workflows", json=body1)
    body2 = make_workflow([t("a"), t("b", ["a"])], key="idem-x")
    resp = client.post("/workflows", json=body2)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "idempotency_conflict"


def test_graph_immutable_no_update_endpoint(client):
    body = make_workflow([t("a")], key="imm-1")
    wf = client.post("/workflows", json=body).json()
    # 没有任何修改图的路由；重复提交仅返回原图
    again = client.post("/workflows", json=body).json()
    assert len(again["tasks"]) == 1
    assert again["id"] == wf["id"]
