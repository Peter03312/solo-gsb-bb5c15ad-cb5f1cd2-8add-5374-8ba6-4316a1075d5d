"""End-to-end tests against the HTTP API (FastAPI TestClient)."""
from __future__ import annotations

import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "test.db"),
        default_lease_seconds=30,
        max_lease_seconds=3600,
    )
    with TestClient(create_app(settings)) as c:
        yield c


def dag_payload(key: str = "wf-1") -> dict:
    return {
        "idempotency_key": key,
        "tasks": [
            {"task_id": "extract", "max_attempts": 2},
            {"task_id": "transform", "depends_on": ["extract"], "max_attempts": 2},
            {"task_id": "load", "depends_on": ["transform"], "max_attempts": 1},
            {"task_id": "audit", "depends_on": ["extract"], "max_attempts": 1},
        ],
    }


def task_map(workflow: dict) -> dict:
    return {t["task_id"]: t for t in workflow["tasks"]}


def submit(client: TestClient, payload: dict | None = None) -> dict:
    resp = client.post("/workflows", json=payload or dag_payload())
    assert resp.status_code == 201, resp.text
    return resp.json()


# --------------------------------------------------------------------- #
# Submission & validation
# --------------------------------------------------------------------- #

def test_submit_initializes_statuses(client):
    wf = submit(client)
    tasks = task_map(wf)
    assert wf["status"] == "RUNNING"
    assert tasks["extract"]["status"] == "READY"
    assert tasks["transform"]["status"] == "PENDING"
    assert tasks["load"]["status"] == "PENDING"
    assert tasks["audit"]["status"] == "PENDING"
    assert all(t["attempts"] == 0 for t in tasks.values())


def test_reject_duplicate_task_ids(client):
    resp = client.post("/workflows", json={
        "idempotency_key": "dup",
        "tasks": [{"task_id": "a"}, {"task_id": "a"}],
    })
    assert resp.status_code == 400
    assert "duplicate" in resp.json()["detail"]


def test_reject_missing_dependency(client):
    resp = client.post("/workflows", json={
        "idempotency_key": "missing",
        "tasks": [{"task_id": "a", "depends_on": ["ghost"]}],
    })
    assert resp.status_code == 400
    assert "unknown task" in resp.json()["detail"]


@pytest.mark.parametrize("tasks", [
    [{"task_id": "a", "depends_on": ["a"]}],                      # self loop
    [{"task_id": "a", "depends_on": ["b"]},
     {"task_id": "b", "depends_on": ["a"]}],                      # 2-cycle
    [{"task_id": "a", "depends_on": ["c"]},
     {"task_id": "b", "depends_on": ["a"]},
     {"task_id": "c", "depends_on": ["b"]}],                      # 3-cycle
])
def test_reject_cycles(client, tasks):
    resp = client.post("/workflows", json={
        "idempotency_key": "cycle", "tasks": tasks,
    })
    assert resp.status_code == 400
    assert "cycle" in resp.json()["detail"]


def test_reject_empty_task_list(client):
    resp = client.post("/workflows", json={"idempotency_key": "empty", "tasks": []})
    assert resp.status_code == 422


# --------------------------------------------------------------------- #
# Idempotency
# --------------------------------------------------------------------- #

def test_idempotent_replay_returns_original_workflow(client):
    first = submit(client)
    resp = client.post("/workflows", json=dag_payload())
    assert resp.status_code == 200
    assert resp.json()["workflow_id"] == first["workflow_id"]
    assert len(client.get("/workflows").json()) == 1


def test_idempotency_key_with_different_request_conflicts(client):
    submit(client)
    changed = dag_payload()
    changed["tasks"][0]["max_attempts"] = 5
    resp = client.post("/workflows", json=changed)
    assert resp.status_code == 409


# --------------------------------------------------------------------- #
# Claiming
# --------------------------------------------------------------------- #

def test_claim_only_ready_tasks(client):
    wf = submit(client)
    wf_id = wf["workflow_id"]

    # 'transform' is PENDING (dependency not satisfied) -> cannot claim it.
    resp = client.post(f"/workflows/{wf_id}/claim", json={"task_id": "transform"})
    assert resp.status_code == 409

    claim = client.post(f"/workflows/{wf_id}/claim").json()
    assert claim["task_id"] == "extract"
    assert claim["attempt"] == 1
    assert claim["lease_token"]
    assert claim["lease_expires_at"]

    # Already claimed -> no other READY task remains.
    resp = client.post(f"/workflows/{wf_id}/claim")
    assert resp.status_code == 404


def test_concurrent_claims_never_double_assign(client):
    wf = submit(client, {
        "idempotency_key": "race",
        "tasks": [{"task_id": "only"}],
    })
    wf_id = wf["workflow_id"]
    results = []

    def worker():
        results.append(client.post(f"/workflows/{wf_id}/claim").status_code)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) == 1
    assert results.count(404) == 7
    tasks = task_map(client.get(f"/workflows/{wf_id}").json())
    assert tasks["only"]["attempts"] == 1


def test_claim_unknown_workflow_or_task(client):
    assert client.post("/workflows/nope/claim").status_code == 404
    wf = submit(client)
    resp = client.post(f"/workflows/{wf['workflow_id']}/claim",
                       json={"task_id": "ghost"})
    assert resp.status_code == 404


# --------------------------------------------------------------------- #
# Reporting & retries
# --------------------------------------------------------------------- #

def claim_one(client, wf_id, lease_seconds=None):
    body = {"lease_seconds": lease_seconds} if lease_seconds else {}
    resp = client.post(f"/workflows/{wf_id}/claim", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


def report(client, wf_id, claim, outcome):
    return client.post(
        f"/workflows/{wf_id}/tasks/{claim['task_id']}/result",
        json={"lease_token": claim["lease_token"], "outcome": outcome},
    )


def test_success_unlocks_dependents_and_completes_workflow(client):
    wf = submit(client)
    wf_id = wf["workflow_id"]

    c1 = claim_one(client, wf_id)
    assert c1["task_id"] == "extract"
    resp = report(client, wf_id, c1, "success")
    assert resp.status_code == 200
    tasks = task_map(resp.json())
    assert tasks["extract"]["status"] == "SUCCEEDED"
    assert tasks["transform"]["status"] == "READY"
    assert tasks["audit"]["status"] == "READY"
    assert tasks["load"]["status"] == "PENDING"

    # 'load' stays locked until 'transform' succeeds.
    resp = client.post(f"/workflows/{wf_id}/claim", json={"task_id": "load"})
    assert resp.status_code == 409

    for task_id in ("transform", "audit", "load"):
        resp = client.post(f"/workflows/{wf_id}/claim", json={"task_id": task_id})
        assert resp.status_code == 200, resp.text
        c = resp.json()
        assert report(client, wf_id, c, "success").status_code == 200

    final = client.get(f"/workflows/{wf_id}").json()
    assert final["status"] == "SUCCEEDED"


def test_report_requires_valid_token(client):
    wf = submit(client)
    wf_id = wf["workflow_id"]
    claim = claim_one(client, wf_id)

    resp = client.post(
        f"/workflows/{wf_id}/tasks/{claim['task_id']}/result",
        json={"lease_token": "forged", "outcome": "success"},
    )
    assert resp.status_code == 403

    # Second claim of the same task fails; the original token still works.
    assert report(client, wf_id, claim, "success").status_code == 200


def test_failure_retries_until_attempts_exhausted_then_skips_successors(client):
    # 'audit' is an independent branch; extract -> transform -> load is another.
    wf = submit(client, {
        "idempotency_key": "retry",
        "tasks": [
            {"task_id": "extract", "max_attempts": 2},
            {"task_id": "transform", "depends_on": ["extract"]},
            {"task_id": "load", "depends_on": ["transform"]},
            {"task_id": "audit"},
        ],
    })
    wf_id = wf["workflow_id"]

    def claim_task(task_id):
        resp = client.post(f"/workflows/{wf_id}/claim", json={"task_id": task_id})
        assert resp.status_code == 200, resp.text
        return resp.json()

    # extract has max_attempts=2: first failure -> READY again.
    c1 = claim_task("extract")
    assert c1["attempt"] == 1
    assert report(client, wf_id, c1, "failure").status_code == 200
    tasks = task_map(client.get(f"/workflows/{wf_id}").json())
    assert tasks["extract"]["status"] == "READY"

    # Second failure exhausts the budget -> FAILED, successors SKIPPED,
    # but the independent 'audit' branch keeps going.
    c2 = claim_task("extract")
    assert c2["attempt"] == 2
    final = report(client, wf_id, c2, "failure").json()
    tasks = task_map(final)
    assert tasks["extract"]["status"] == "FAILED"
    assert tasks["transform"]["status"] == "SKIPPED"
    assert tasks["load"]["status"] == "SKIPPED"
    assert tasks["audit"]["status"] == "READY"
    assert final["status"] == "RUNNING"

    # Other branch still progresses; workflow ends FAILED overall.
    c3 = claim_task("audit")
    final = report(client, wf_id, c3, "success").json()
    assert final["status"] == "FAILED"


def test_lease_expiry_makes_task_reclaimable_and_invalidates_old_token(client):
    wf = submit(client, {
        "idempotency_key": "lease",
        "tasks": [{"task_id": "a", "max_attempts": 2},
                  {"task_id": "b", "depends_on": ["a"]}],
    })
    wf_id = wf["workflow_id"]

    stale = claim_one(client, wf_id, lease_seconds=1)
    time.sleep(1.2)

    fresh = claim_one(client, wf_id)
    assert fresh["task_id"] == "a" and fresh["attempt"] == 2

    # The expired token can no longer report.
    resp = report(client, wf_id, stale, "success")
    assert resp.status_code == 403

    assert report(client, wf_id, fresh, "success").status_code == 200
    tasks = task_map(client.get(f"/workflows/{wf_id}").json())
    assert tasks["b"]["status"] == "READY"


def test_lease_expiry_on_last_attempt_fails_task_and_skips_successors(client):
    wf = submit(client, {
        "idempotency_key": "lease-exhaust",
        "tasks": [{"task_id": "a", "max_attempts": 1},
                  {"task_id": "b", "depends_on": ["a"]}],
    })
    wf_id = wf["workflow_id"]

    claim_one(client, wf_id, lease_seconds=1)
    time.sleep(1.2)

    final = client.get(f"/workflows/{wf_id}").json()
    tasks = task_map(final)
    assert tasks["a"]["status"] == "FAILED"
    assert tasks["b"]["status"] == "SKIPPED"
    assert final["status"] == "FAILED"


# --------------------------------------------------------------------- #
# Cancellation
# --------------------------------------------------------------------- #

def test_cancel_blocks_claims_and_stale_tokens(client):
    wf = submit(client)
    wf_id = wf["workflow_id"]
    claim = claim_one(client, wf_id)

    cancelled = client.post(f"/workflows/{wf_id}/cancel")
    assert cancelled.status_code == 200
    tasks = task_map(cancelled.json())
    assert cancelled.json()["status"] == "CANCELLED"
    assert tasks["extract"]["status"] == "CANCELLED"
    assert tasks["transform"]["status"] == "CANCELLED"

    assert client.post(f"/workflows/{wf_id}/claim").status_code == 409
    assert report(client, wf_id, claim, "success").status_code == 409

    # Cancel is idempotent; completed workflows cannot be cancelled.
    assert client.post(f"/workflows/{wf_id}/cancel").status_code == 200


def test_cancel_terminal_workflow_conflicts(client):
    wf = submit(client, {"idempotency_key": "done", "tasks": [{"task_id": "a"}]})
    wf_id = wf["workflow_id"]
    report(client, wf_id, claim_one(client, wf_id), "success")
    assert client.post(f"/workflows/{wf_id}/cancel").status_code == 409


# --------------------------------------------------------------------- #
# Persistence across restarts
# --------------------------------------------------------------------- #

def test_state_and_leases_survive_restart(tmp_path):
    db_path = str(tmp_path / "restart.db")
    settings = Settings(database_path=db_path, default_lease_seconds=30,
                        max_lease_seconds=3600)

    with TestClient(create_app(settings)) as c1:
        wf = submit(c1, {
            "idempotency_key": "restart",
            "tasks": [{"task_id": "a"}, {"task_id": "b", "depends_on": ["a"]}],
        })
        wf_id = wf["workflow_id"]
        claim = claim_one(c1, wf_id)

    # New app instance over the same database file == service restart.
    with TestClient(create_app(settings)) as c2:
        restored = c2.get(f"/workflows/{wf_id}").json()
        tasks = task_map(restored)
        assert tasks["a"]["status"] == "RUNNING"
        assert tasks["a"]["lease_expires_at"] is not None
        assert tasks["b"]["status"] == "PENDING"

        # The lease token issued before the restart is still valid.
        assert report(c2, wf_id, claim, "success").status_code == 200
        tasks = task_map(c2.get(f"/workflows/{wf_id}").json())
        assert tasks["a"]["status"] == "SUCCEEDED"
        assert tasks["b"]["status"] == "READY"


# --------------------------------------------------------------------- #
# Misc
# --------------------------------------------------------------------- #

def test_global_claim_endpoint(client):
    wf = submit(client, {"idempotency_key": "global", "tasks": [{"task_id": "x"}]})
    resp = client.post("/claims", json={})
    assert resp.status_code == 200
    assert resp.json()["workflow_id"] == wf["workflow_id"]
    assert resp.json()["task_id"] == "x"
    assert client.post("/claims", json={}).status_code == 404


def test_get_unknown_workflow(client):
    assert client.get("/workflows/nope").status_code == 404
