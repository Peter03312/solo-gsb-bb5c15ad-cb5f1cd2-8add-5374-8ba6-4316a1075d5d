"""核心业务逻辑。

所有写操作都在 ``BEGIN IMMEDIATE`` 事务中完成，配合 SQLite WAL 与
busy_timeout：多工作进程/多线程并发领取时，同一条任务只会被一个
条件 UPDATE 抢到，天然不会重复分配。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .errors import ServiceError
from .models import WorkflowIn

TERMINAL_TASK_STATUSES = ("succeeded", "failed", "skipped", "cancelled")
ACTIVE_TASK_STATUSES = ("pending", "leased")


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def new_token() -> str:
    return secrets.token_urlsafe(32)


@contextmanager
def transaction(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- #
# 图校验
# --------------------------------------------------------------------------- #
def validate_graph(payload: WorkflowIn) -> tuple[list[str], dict[str, list[str]]]:
    """返回 (有序任务标识, 标识->去重后依赖标识)；非法图抛 422。"""
    keys: list[str] = []
    seen: set[str] = set()
    raw_deps: dict[str, list[str]] = {}

    for task in payload.tasks:
        if task.key in seen:
            raise ServiceError(
                "duplicate_task_key",
                f"任务标识重复: {task.key}",
                422,
                {"key": task.key},
            )
        seen.add(task.key)
        keys.append(task.key)

        normalized: list[str] = []
        dep_seen: set[str] = set()
        for dep in task.dependencies:
            if dep == task.key:
                raise ServiceError(
                    "dependency_cycle",
                    f"任务 {task.key} 依赖自身，构成环",
                    422,
                    {"key": task.key},
                )
            if dep not in dep_seen:
                dep_seen.add(dep)
                normalized.append(dep)
        raw_deps[task.key] = normalized

    # 缺失依赖
    for key, deps in raw_deps.items():
        for dep in deps:
            if dep not in seen:
                raise ServiceError(
                    "missing_dependency",
                    f"任务 {key} 依赖的任务 {dep} 不存在",
                    422,
                    {"key": key, "dependency": dep},
                )

    # Kahn 拓扑排序检测环
    dependents: dict[str, list[str]] = {k: [] for k in keys}
    indegree: dict[str, int] = {}
    for key, deps in raw_deps.items():
        unique_deps = set(deps)
        indegree[key] = len(unique_deps)
        for dep in unique_deps:
            dependents[dep].append(key)

    queue = deque(k for k in keys if indegree[k] == 0)
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for nxt in dependents[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if visited != len(keys):
        cyclic = sorted(k for k in keys if indegree[k] > 0)
        raise ServiceError(
            "dependency_cycle",
            "任务依赖图存在环，工作流拒绝受理",
            422,
            {"tasks_in_cycle": cyclic},
        )

    return keys, raw_deps


def request_fingerprint(payload: WorkflowIn) -> str:
    """幂等键以外的请求体规范化哈希，用于区分“重复提交/冲突提交”。"""
    body = payload.model_dump(exclude={"idempotency_key"})
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return _sha256(canonical)


# --------------------------------------------------------------------------- #
# 提交工作流（图接受后不可改：无任何更新任务图的接口）
# --------------------------------------------------------------------------- #
def create_workflow(
    conn: sqlite3.Connection, payload: WorkflowIn, settings: Settings
) -> tuple[dict[str, Any], bool]:
    keys, deps = validate_graph(payload)
    fingerprint = request_fingerprint(payload)

    with transaction(conn):
        existing = conn.execute(
            "SELECT id FROM workflows WHERE idempotency_key = ?",
            (payload.idempotency_key,),
        ).fetchone()
        if existing is not None:
            row = conn.execute(
                "SELECT request_hash FROM workflows WHERE id = ?",
                (existing["id"],),
            ).fetchone()
            if row["request_hash"] != fingerprint:
                raise ServiceError(
                    "idempotency_conflict",
                    "同一幂等键已用于不同的请求内容",
                    409,
                    {
                        "idempotency_key": payload.idempotency_key,
                        "existing_workflow_id": existing["id"],
                    },
                )
            return _load_detail(conn, existing["id"]), True

        wf_id = uuid.uuid4().hex
        now = utcnow_iso()
        conn.execute(
            """INSERT INTO workflows
               (id, idempotency_key, request_hash, status,
                default_max_attempts, created_at, updated_at)
               VALUES (?, ?, ?, 'pending', ?, ?, ?)""",
            (
                wf_id,
                payload.idempotency_key,
                fingerprint,
                payload.max_attempts,
                now,
                now,
            ),
        )

        task_ids: dict[str, str] = {}
        for key in keys:
            task_ids[key] = uuid.uuid4().hex

        for task in payload.tasks:
            max_attempts = (
                task.max_attempts
                if task.max_attempts is not None
                else payload.max_attempts
            )
            conn.execute(
                """INSERT INTO tasks
                   (id, workflow_id, task_key, max_attempts, attempts, status,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, 0, 'pending', ?, ?)""",
                (
                    task_ids[task.key],
                    wf_id,
                    task.key,
                    max_attempts,
                    now,
                    now,
                ),
            )
            for dep in deps[task.key]:
                conn.execute(
                    """INSERT OR IGNORE INTO task_dependencies
                       (workflow_id, task_id, depends_on_id)
                       VALUES (?, ?, ?)""",
                    (wf_id, task_ids[task.key], task_ids[dep]),
                )

        return _load_detail(conn, wf_id), False


# --------------------------------------------------------------------------- #
# 租约到期 / 级联 / 终结判定
# --------------------------------------------------------------------------- #
def expire_leases(conn: sqlite3.Connection, now: str) -> set[str]:
    """把已到期租约复位；达到上限的置失败并级联跳过后继。

    返回被改动任务所属的工作流 id 集合（供终结判定）。
    """
    expired = conn.execute(
        "SELECT id, workflow_id, attempts, max_attempts FROM tasks "
        "WHERE status = 'leased' AND leased_until IS NOT NULL "
        "AND leased_until <= ?",
        (now,),
    ).fetchall()

    failed_roots: list[str] = []
    affected_workflows: set[str] = set()
    for row in expired:
        affected_workflows.add(row["workflow_id"])
        if row["attempts"] >= row["max_attempts"]:
            conn.execute(
                "UPDATE tasks SET status = 'failed', token_hash = NULL, "
                "leased_until = NULL, updated_at = ? WHERE id = ?",
                (now, row["id"]),
            )
            failed_roots.append(row["id"])
        else:
            conn.execute(
                "UPDATE tasks SET status = 'pending', token_hash = NULL, "
                "leased_until = NULL, updated_at = ? WHERE id = ?",
                (now, row["id"]),
            )

    for root_id in failed_roots:
        affected_workflows.update(cascade_skip(conn, root_id, now))
    return affected_workflows


def cascade_skip(
    conn: sqlite3.Connection, failed_task_id: str, now: str
) -> set[str]:
    """把一个最终失败任务的所有“未完成后继”递归置为 skipped。

    其他分支（不依赖失败链的任务）保持可领取。
    """
    affected: set[str] = set()
    queue = deque([failed_task_id])
    while queue:
        parent = queue.popleft()
        children = conn.execute(
            """SELECT t.id, t.workflow_id, t.status
               FROM task_dependencies d JOIN tasks t ON t.id = d.task_id
               WHERE d.depends_on_id = ?""",
            (parent,),
        ).fetchall()
        for child in children:
            if child["status"] in ACTIVE_TASK_STATUSES:
                conn.execute(
                    "UPDATE tasks SET status = 'skipped', token_hash = NULL, "
                    "leased_until = NULL, updated_at = ? WHERE id = ?",
                    (now, child["id"]),
                )
                affected.add(child["workflow_id"])
                queue.append(child["id"])
    return affected


def finalize_workflow(conn: sqlite3.Connection, workflow_id: str, now: str) -> None:
    wf = conn.execute(
        "SELECT status FROM workflows WHERE id = ?", (workflow_id,)
    ).fetchone()
    if wf is None or wf["status"] not in ("pending", "running"):
        return

    counts: dict[str, int] = defaultdict(int)
    rows = conn.execute(
        "SELECT status, COUNT(*) AS c FROM tasks WHERE workflow_id = ? GROUP BY status",
        (workflow_id,),
    ).fetchall()
    for row in rows:
        counts[row["status"]] = row["c"]
    total = sum(counts.values())
    active = counts["pending"] + counts["leased"]

    new_status: str | None = None
    if active > 0:
        if counts["leased"] > 0 and wf["status"] == "pending":
            new_status = "running"
    elif counts["succeeded"] == total:
        new_status = "succeeded"
    else:
        new_status = "failed"

    if new_status is not None and new_status != wf["status"]:
        conn.execute(
            "UPDATE workflows SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, now, workflow_id),
        )


# --------------------------------------------------------------------------- #
# 领取任务
# --------------------------------------------------------------------------- #
def claim_task(
    conn: sqlite3.Connection,
    settings: Settings,
    now: str,
    lease_seconds: int | None = None,
    workflow_id: str | None = None,
) -> dict[str, Any] | None:
    lease_seconds = lease_seconds or settings.lease_seconds
    deadline_ts = datetime.now(timezone.utc).timestamp() + lease_seconds
    deadline_iso = (
        datetime.fromtimestamp(deadline_ts, tz=timezone.utc)
        .isoformat(timespec="seconds")
    )

    with transaction(conn):
        affected = expire_leases(conn, now)

        if workflow_id is not None:
            wf = conn.execute(
                "SELECT id FROM workflows WHERE id = ?", (workflow_id,)
            ).fetchone()
            if wf is None:
                raise ServiceError(
                    "workflow_not_found", "工作流不存在", 404
                )

        # 依赖全部成功的 pending 任务；NOT EXISTS 保证任依赖未成功即不可领
        sql = (
            "SELECT t.id, t.workflow_id FROM tasks t "
            "WHERE t.status = 'pending' "
            + ("AND t.workflow_id = ? " if workflow_id else "")
            + "AND NOT EXISTS ("
            "  SELECT 1 FROM task_dependencies d "
            "  JOIN tasks p ON p.id = d.depends_on_id "
            "  WHERE d.task_id = t.id AND p.status != 'succeeded'"
            ") ORDER BY t.created_at ASC, t.task_key ASC, t.id ASC LIMIT 1"
        )
        params: tuple[Any, ...] = (
            (workflow_id,) if workflow_id else ()
        )
        candidate = conn.execute(sql, params).fetchone()
        if candidate is None:
            for wf_id in affected:
                finalize_workflow(conn, wf_id, now)
            return None

        token = new_token()
        cur = conn.execute(
            """UPDATE tasks
               SET attempts = attempts + 1, status = 'leased',
                   token_hash = ?, leased_until = ?, updated_at = ?
               WHERE id = ? AND status = 'pending'""",
            (_sha256(token), deadline_iso, now, candidate["id"]),
        )
        if cur.rowcount != 1:
            # 被并发工作进程抢先领取：本次无任务
            return None

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (candidate["id"],)
        ).fetchone()
        conn.execute(
            "UPDATE workflows SET status = 'running', updated_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (now, task["workflow_id"]),
        )
        for wf_id in affected | {task["workflow_id"]}:
            finalize_workflow(conn, wf_id, now)

        return {
            "workflow_id": task["workflow_id"],
            "task_id": task["id"],
            "key": task["task_key"],
            "attempt": task["attempts"],
            "max_attempts": task["max_attempts"],
            "token": token,
            "leased_until": deadline_iso,
            "deadline_seconds": lease_seconds,
        }


# --------------------------------------------------------------------------- #
# 上报结果
# --------------------------------------------------------------------------- #
def report_task(
    conn: sqlite3.Connection,
    settings: Settings,
    task_id: str,
    token: str,
    success: bool,
    error: str | None,
    result: str | None,
    now: str,
) -> dict[str, Any]:
    with transaction(conn):
        affected = expire_leases(conn, now)

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if task is None:
            raise ServiceError("task_not_found", "任务不存在", 404)

        wf = conn.execute(
            "SELECT status FROM workflows WHERE id = ?",
            (task["workflow_id"],),
        ).fetchone()

        if not task["token_hash"] or not hmac.compare_digest(
            task["token_hash"], _sha256(token)
        ):
            raise ServiceError(
                "invalid_token",
                "令牌无效或已失效（租约到期、任务被重领或工作流取消）",
                403,
            )
        if task["status"] != "leased":
            raise ServiceError(
                "not_leased",
                f"任务当前状态为 {task['status']}，不能上报",
                409,
                {"status": task["status"]},
            )
        if wf["status"] == "cancelled":
            raise ServiceError(
                "workflow_cancelled", "工作流已取消，旧令牌提交被拒绝", 409
            )

        if success:
            conn.execute(
                """UPDATE tasks
                   SET status = 'succeeded', token_hash = NULL,
                       leased_until = NULL, result = ?, error = NULL,
                       updated_at = ?
                   WHERE id = ?""",
                (result, now, task_id),
            )
        else:
            message = error or "worker reported failure"
            if task["attempts"] >= task["max_attempts"]:
                conn.execute(
                    """UPDATE tasks
                       SET status = 'failed', token_hash = NULL,
                           leased_until = NULL, error = ?, updated_at = ?
                       WHERE id = ?""",
                    (message, now, task_id),
                )
                cascade_skip(conn, task_id, now)
            else:
                conn.execute(
                    """UPDATE tasks
                       SET status = 'pending', token_hash = NULL,
                           leased_until = NULL, error = ?, updated_at = ?
                       WHERE id = ?""",
                    (message, now, task_id),
                )

        affected.add(task["workflow_id"])
        for wf_id in affected:
            finalize_workflow(conn, wf_id, now)

        row = conn.execute(
            "SELECT id, workflow_id, status, attempts FROM tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        return {
            "workflow_id": row["workflow_id"],
            "task_id": row["id"],
            "status": row["status"],
            "attempts": row["attempts"],
        }


# --------------------------------------------------------------------------- #
# 取消
# --------------------------------------------------------------------------- #
def cancel_workflow(
    conn: sqlite3.Connection, workflow_id: str, now: str
) -> dict[str, Any]:
    with transaction(conn):
        wf = conn.execute(
            "SELECT * FROM workflows WHERE id = ?", (workflow_id,)
        ).fetchone()
        if wf is None:
            raise ServiceError("workflow_not_found", "工作流不存在", 404)
        if wf["status"] == "cancelled":
            return {
                "id": workflow_id,
                "status": "cancelled",
                "cancelled_tasks": 0,
                "updated_at": wf["updated_at"],
            }
        if wf["status"] in ("succeeded", "failed"):
            raise ServiceError(
                "workflow_terminal",
                f"工作流已处于终态 {wf['status']}，不能取消",
                409,
                {"status": wf["status"]},
            )

        expire_leases(conn, now)
        conn.execute(
            "UPDATE workflows SET status = 'cancelled', updated_at = ? WHERE id = ?",
            (now, workflow_id),
        )
        cur = conn.execute(
            """UPDATE tasks
               SET status = 'cancelled', token_hash = NULL,
                   leased_until = NULL, updated_at = ?
               WHERE workflow_id = ? AND status IN ('pending', 'leased')""",
            (now, workflow_id),
        )
        return {
            "id": workflow_id,
            "status": "cancelled",
            "cancelled_tasks": cur.rowcount,
            "updated_at": now,
        }


# --------------------------------------------------------------------------- #
# 查询
# --------------------------------------------------------------------------- #
def get_workflow_detail(
    conn: sqlite3.Connection, workflow_id: str, now: str
) -> dict[str, Any]:
    with transaction(conn):
        wf = conn.execute(
            "SELECT * FROM workflows WHERE id = ?", (workflow_id,)
        ).fetchone()
        if wf is None:
            raise ServiceError("workflow_not_found", "工作流不存在", 404)
        affected = expire_leases(conn, now)
        affected.add(workflow_id)
        for wf_id in affected:
            finalize_workflow(conn, wf_id, now)
        return _load_detail(conn, workflow_id)


def list_workflows(
    conn: sqlite3.Connection, limit: int, offset: int
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id FROM workflows ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    return [_load_detail(conn, r["id"]) for r in rows]


def _load_detail(conn: sqlite3.Connection, workflow_id: str) -> dict[str, Any]:
    wf = conn.execute(
        "SELECT * FROM workflows WHERE id = ?", (workflow_id,)
    ).fetchone()
    task_rows = conn.execute(
        "SELECT * FROM tasks WHERE workflow_id = ? ORDER BY created_at ASC, id ASC",
        (workflow_id,),
    ).fetchall()
    id_to_key = {r["id"]: r["task_key"] for r in task_rows}
    dep_map: dict[str, list[str]] = {r["id"]: [] for r in task_rows}
    dep_rows = conn.execute(
        "SELECT task_id, depends_on_id FROM task_dependencies WHERE workflow_id = ?",
        (workflow_id,),
    ).fetchall()
    for d in dep_rows:
        dep_map[d["task_id"]].append(id_to_key[d["depends_on_id"]])

    tasks = []
    for r in task_rows:
        tasks.append(
            {
                "id": r["id"],
                "key": r["task_key"],
                "status": r["status"],
                "dependencies": sorted(dep_map[r["id"]]),
                "attempts": r["attempts"],
                "max_attempts": r["max_attempts"],
                "leased_until": r["leased_until"],
                "result": r["result"],
                "error": r["error"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
            }
        )

    return {
        "id": wf["id"],
        "status": wf["status"],
        "idempotency_key": wf["idempotency_key"],
        "default_max_attempts": wf["default_max_attempts"],
        "created_at": wf["created_at"],
        "updated_at": wf["updated_at"],
        "tasks": tasks,
    }
