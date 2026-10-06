"""Core orchestration logic.

Task lifecycle:
    PENDING -> READY -> RUNNING -> SUCCEEDED
                       RUNNING -> READY      (failure / lease expiry, attempts left)
                       RUNNING -> FAILED     (attempts exhausted)
    PENDING/READY -> SKIPPED                 (an ancestor failed permanently)
    PENDING/READY/RUNNING -> CANCELLED       (workflow cancelled)

Attempts are counted at claim time. A claim hands out a lease token with an
expiry; only the current token holder may report the outcome. Expired leases
are reaped lazily on every operation touching the workflow, so state survives
restarts without a background sweeper.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone

from .db import Database
from .schemas import SubmitWorkflowRequest

# Task statuses
PENDING = "PENDING"
READY = "READY"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
SKIPPED = "SKIPPED"
CANCELLED = "CANCELLED"

# Workflow statuses
WF_RUNNING = "RUNNING"
WF_SUCCEEDED = "SUCCEEDED"
WF_FAILED = "FAILED"
WF_CANCELLED = "CANCELLED"


class ValidationError(Exception):
    """400 - the submitted graph is invalid."""


class NotFoundError(Exception):
    """404 - workflow/task does not exist, or nothing is claimable."""


class ConflictError(Exception):
    """409 - idempotency conflict or illegal state transition."""


class ForbiddenError(Exception):
    """403 - lease token does not match the current holder."""


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


class WorkflowService:
    def __init__(self, db: Database, default_lease_seconds: int = 30,
                 max_lease_seconds: int = 3600):
        self.db = db
        self.default_lease_seconds = default_lease_seconds
        self.max_lease_seconds = max_lease_seconds

    # ------------------------------------------------------------------ #
    # Submission
    # ------------------------------------------------------------------ #

    def submit_workflow(self, req: SubmitWorkflowRequest) -> tuple[dict, bool]:
        """Returns (workflow_view, created). Replay returns created=False."""
        self._validate_graph(req)
        request_hash = self._hash_request(req)
        now = time.time()
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM workflows WHERE idempotency_key = ?",
                (req.idempotency_key,),
            ).fetchone()
            if row is not None:
                if row["request_hash"] != request_hash:
                    raise ConflictError(
                        "idempotency_key was already used with a different request"
                    )
                return self._workflow_view(conn, row["workflow_id"]), False

            workflow_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO workflows (workflow_id, idempotency_key, request_hash,"
                " status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (workflow_id, req.idempotency_key, request_hash, WF_RUNNING, now, now),
            )
            for task in req.tasks:
                status = READY if not task.depends_on else PENDING
                conn.execute(
                    "INSERT INTO tasks (workflow_id, task_id, status, attempts,"
                    " max_attempts, lease_token, lease_expires_at, created_at,"
                    " updated_at) VALUES (?, ?, ?, 0, ?, NULL, NULL, ?, ?)",
                    (workflow_id, task.task_id, status, task.max_attempts, now, now),
                )
                for dep in sorted(set(task.depends_on)):
                    conn.execute(
                        "INSERT INTO task_deps (workflow_id, task_id, depends_on)"
                        " VALUES (?, ?, ?)",
                        (workflow_id, task.task_id, dep),
                    )
            return self._workflow_view(conn, workflow_id), True

    @staticmethod
    def _validate_graph(req: SubmitWorkflowRequest) -> None:
        ids = [t.task_id for t in req.tasks]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ValidationError(f"duplicate task_id(s): {', '.join(duplicates)}")

        known = set(ids)
        for task in req.tasks:
            for dep in task.depends_on:
                if dep not in known:
                    raise ValidationError(
                        f"task '{task.task_id}' depends on unknown task '{dep}'"
                    )

        # Kahn's algorithm: any unprocessed node means the graph has a cycle.
        indegree = {t.task_id: len(set(t.depends_on)) for t in req.tasks}
        dependents: dict[str, list[str]] = {t.task_id: [] for t in req.tasks}
        for t in req.tasks:
            for dep in set(t.depends_on):
                dependents[dep].append(t.task_id)
        queue = [tid for tid, deg in indegree.items() if deg == 0]
        processed = 0
        while queue:
            tid = queue.pop()
            processed += 1
            for child in dependents[tid]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        if processed != len(req.tasks):
            raise ValidationError("dependency graph contains a cycle")

    @staticmethod
    def _hash_request(req: SubmitWorkflowRequest) -> str:
        canonical = {
            "tasks": sorted(
                (
                    {
                        "task_id": t.task_id,
                        "depends_on": sorted(set(t.depends_on)),
                        "max_attempts": t.max_attempts,
                    }
                    for t in req.tasks
                ),
                key=lambda t: t["task_id"],
            )
        }
        blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def get_workflow(self, workflow_id: str) -> dict:
        with self.db.transaction() as conn:
            self._require_workflow(conn, workflow_id)
            self._reap_expired(conn, time.time(), workflow_id=workflow_id)
            return self._workflow_view(conn, workflow_id)

    def list_workflows(self) -> list[dict]:
        with self.db.transaction() as conn:
            self._reap_expired(conn, time.time())
            rows = conn.execute(
                "SELECT * FROM workflows ORDER BY created_at, workflow_id"
            ).fetchall()
            return [
                {
                    "workflow_id": r["workflow_id"],
                    "idempotency_key": r["idempotency_key"],
                    "status": r["status"],
                    "created_at": _iso(r["created_at"]),
                    "updated_at": _iso(r["updated_at"]),
                }
                for r in rows
            ]

    # ------------------------------------------------------------------ #
    # Claiming
    # ------------------------------------------------------------------ #

    def claim(self, workflow_id: str, task_id: str | None = None,
              lease_seconds: int | None = None) -> dict:
        lease = self._clamp_lease(lease_seconds)
        now = time.time()
        with self.db.transaction() as conn:
            wf = self._require_workflow(conn, workflow_id)
            self._assert_accepting_work(wf)
            self._reap_expired(conn, now, workflow_id=workflow_id)

            if task_id is not None:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE workflow_id = ? AND task_id = ?",
                    (workflow_id, task_id),
                ).fetchone()
                if row is None:
                    raise NotFoundError(
                        f"task '{task_id}' not found in workflow '{workflow_id}'"
                    )
                if row["status"] != READY:
                    raise ConflictError(
                        f"task '{task_id}' is not claimable (status={row['status']})"
                    )
                chosen = task_id
            else:
                row = conn.execute(
                    "SELECT task_id FROM tasks WHERE workflow_id = ? AND status = ?"
                    " ORDER BY created_at, task_id LIMIT 1",
                    (workflow_id, READY),
                ).fetchone()
                if row is None:
                    raise NotFoundError("no claimable tasks")
                chosen = row["task_id"]

            return self._assign(conn, workflow_id, chosen, lease, now)

    def claim_any(self, lease_seconds: int | None = None) -> dict:
        """Claim the oldest READY task across all running workflows."""
        lease = self._clamp_lease(lease_seconds)
        now = time.time()
        with self.db.transaction() as conn:
            self._reap_expired(conn, now)
            row = conn.execute(
                "SELECT t.workflow_id, t.task_id FROM tasks t"
                " JOIN workflows w ON w.workflow_id = t.workflow_id"
                " WHERE w.status = ? AND t.status = ?"
                " ORDER BY w.created_at, t.created_at, t.task_id LIMIT 1",
                (WF_RUNNING, READY),
            ).fetchone()
            if row is None:
                raise NotFoundError("no claimable tasks")
            return self._assign(conn, row["workflow_id"], row["task_id"], lease, now)

    def _assign(self, conn: sqlite3.Connection, workflow_id: str, task_id: str,
                lease: int, now: float) -> dict:
        token = uuid.uuid4().hex
        expires_at = now + lease
        # Conditional UPDATE: even if two transactions raced, only the one
        # that still sees status=READY flips the task to RUNNING.
        cur = conn.execute(
            "UPDATE tasks SET status = ?, attempts = attempts + 1, lease_token = ?,"
            " lease_expires_at = ?, updated_at = ?"
            " WHERE workflow_id = ? AND task_id = ? AND status = ?",
            (RUNNING, token, expires_at, now, workflow_id, task_id, READY),
        )
        if cur.rowcount != 1:
            raise ConflictError(f"task '{task_id}' was claimed concurrently")
        row = conn.execute(
            "SELECT attempts FROM tasks WHERE workflow_id = ? AND task_id = ?",
            (workflow_id, task_id),
        ).fetchone()
        return {
            "workflow_id": workflow_id,
            "task_id": task_id,
            "attempt": row["attempts"],
            "lease_token": token,
            "lease_expires_at": _iso(expires_at),
        }

    def _clamp_lease(self, lease_seconds: int | None) -> int:
        lease = lease_seconds if lease_seconds is not None else self.default_lease_seconds
        return max(1, min(lease, self.max_lease_seconds))

    # ------------------------------------------------------------------ #
    # Reporting
    # ------------------------------------------------------------------ #

    def report_result(self, workflow_id: str, task_id: str, lease_token: str,
                      outcome: str) -> dict:
        now = time.time()
        with self.db.transaction() as conn:
            wf = self._require_workflow(conn, workflow_id)
            if wf["status"] == WF_CANCELLED:
                raise ConflictError(
                    "workflow is cancelled; results are no longer accepted"
                )
            self._reap_expired(conn, now, workflow_id=workflow_id)

            task = conn.execute(
                "SELECT * FROM tasks WHERE workflow_id = ? AND task_id = ?",
                (workflow_id, task_id),
            ).fetchone()
            if task is None:
                raise NotFoundError(
                    f"task '{task_id}' not found in workflow '{workflow_id}'"
                )
            if task["status"] != RUNNING:
                raise ConflictError(
                    f"task '{task_id}' is not running (status={task['status']});"
                    " the lease may have expired or been reclaimed"
                )
            if task["lease_token"] != lease_token:
                raise ForbiddenError("invalid lease token")

            if outcome == "success":
                conn.execute(
                    "UPDATE tasks SET status = ?, lease_token = NULL,"
                    " lease_expires_at = NULL, updated_at = ?"
                    " WHERE workflow_id = ? AND task_id = ?",
                    (SUCCEEDED, now, workflow_id, task_id),
                )
                self._promote_ready(conn, workflow_id, now)
            elif task["attempts"] >= task["max_attempts"]:
                conn.execute(
                    "UPDATE tasks SET status = ?, lease_token = NULL,"
                    " lease_expires_at = NULL, updated_at = ?"
                    " WHERE workflow_id = ? AND task_id = ?",
                    (FAILED, now, workflow_id, task_id),
                )
                self._cascade_skip(conn, workflow_id, task_id, now)
            else:
                conn.execute(
                    "UPDATE tasks SET status = ?, lease_token = NULL,"
                    " lease_expires_at = NULL, updated_at = ?"
                    " WHERE workflow_id = ? AND task_id = ?",
                    (READY, now, workflow_id, task_id),
                )

            self._refresh_workflow_status(conn, workflow_id, now)
            return self._workflow_view(conn, workflow_id)

    # ------------------------------------------------------------------ #
    # Cancellation
    # ------------------------------------------------------------------ #

    def cancel_workflow(self, workflow_id: str) -> dict:
        now = time.time()
        with self.db.transaction() as conn:
            wf = self._require_workflow(conn, workflow_id)
            if wf["status"] == WF_CANCELLED:
                return self._workflow_view(conn, workflow_id)  # idempotent
            if wf["status"] in (WF_SUCCEEDED, WF_FAILED):
                raise ConflictError(
                    f"cannot cancel a workflow in status {wf['status']}"
                )
            conn.execute(
                "UPDATE workflows SET status = ?, updated_at = ? WHERE workflow_id = ?",
                (WF_CANCELLED, now, workflow_id),
            )
            conn.execute(
                "UPDATE tasks SET status = ?, lease_token = NULL,"
                " lease_expires_at = NULL, updated_at = ?"
                " WHERE workflow_id = ? AND status IN (?, ?, ?)",
                (CANCELLED, now, workflow_id, PENDING, READY, RUNNING),
            )
            return self._workflow_view(conn, workflow_id)

    # ------------------------------------------------------------------ #
    # Internal helpers (must be called inside a transaction)
    # ------------------------------------------------------------------ #

    @staticmethod
    def _require_workflow(conn: sqlite3.Connection, workflow_id: str) -> sqlite3.Row:
        wf = conn.execute(
            "SELECT * FROM workflows WHERE workflow_id = ?", (workflow_id,)
        ).fetchone()
        if wf is None:
            raise NotFoundError(f"workflow '{workflow_id}' not found")
        return wf

    @staticmethod
    def _assert_accepting_work(wf: sqlite3.Row) -> None:
        if wf["status"] == WF_CANCELLED:
            raise ConflictError("workflow is cancelled; claiming is not allowed")
        if wf["status"] in (WF_SUCCEEDED, WF_FAILED):
            raise ConflictError(f"workflow already finished (status={wf['status']})")

    def _reap_expired(self, conn: sqlite3.Connection, now: float,
                      workflow_id: str | None = None) -> None:
        """Move tasks with expired leases back to READY, or fail them when
        the attempt budget is exhausted (and skip their successors)."""
        if workflow_id is not None:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE workflow_id = ? AND status = ?"
                " AND lease_expires_at < ?",
                (workflow_id, RUNNING, now),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status = ? AND lease_expires_at < ?",
                (RUNNING, now),
            ).fetchall()

        touched: set[str] = set()
        for row in rows:
            if row["attempts"] >= row["max_attempts"]:
                conn.execute(
                    "UPDATE tasks SET status = ?, lease_token = NULL,"
                    " lease_expires_at = NULL, updated_at = ?"
                    " WHERE workflow_id = ? AND task_id = ?",
                    (FAILED, now, row["workflow_id"], row["task_id"]),
                )
                self._cascade_skip(conn, row["workflow_id"], row["task_id"], now)
            else:
                conn.execute(
                    "UPDATE tasks SET status = ?, lease_token = NULL,"
                    " lease_expires_at = NULL, updated_at = ?"
                    " WHERE workflow_id = ? AND task_id = ?",
                    (READY, now, row["workflow_id"], row["task_id"]),
                )
            touched.add(row["workflow_id"])
        for wf_id in touched:
            self._refresh_workflow_status(conn, wf_id, now)

    @staticmethod
    def _promote_ready(conn: sqlite3.Connection, workflow_id: str,
                       now: float) -> None:
        """PENDING -> READY for tasks whose dependencies all succeeded."""
        conn.execute(
            "UPDATE tasks SET status = ?, updated_at = ?"
            " WHERE workflow_id = ? AND status = ? AND NOT EXISTS ("
            "   SELECT 1 FROM task_deps d"
            "   JOIN tasks dep ON dep.workflow_id = d.workflow_id"
            "                 AND dep.task_id = d.depends_on"
            "   WHERE d.workflow_id = tasks.workflow_id"
            "     AND d.task_id = tasks.task_id"
            "     AND dep.status != ?"
            " )",
            (READY, now, workflow_id, PENDING, SUCCEEDED),
        )

    def _cascade_skip(self, conn: sqlite3.Connection, workflow_id: str,
                      root_task_id: str, now: float) -> None:
        """Mark every incomplete transitive dependent of a failed task SKIPPED."""
        queue = [root_task_id]
        seen = {root_task_id}
        while queue:
            current = queue.pop()
            rows = conn.execute(
                "SELECT task_id FROM task_deps WHERE workflow_id = ? AND depends_on = ?",
                (workflow_id, current),
            ).fetchall()
            for row in rows:
                tid = row["task_id"]
                if tid in seen:
                    continue
                seen.add(tid)
                cur = conn.execute(
                    "UPDATE tasks SET status = ?, lease_token = NULL,"
                    " lease_expires_at = NULL, updated_at = ?"
                    " WHERE workflow_id = ? AND task_id = ? AND status IN (?, ?)",
                    (SKIPPED, now, workflow_id, tid, PENDING, READY),
                )
                if cur.rowcount == 1:
                    queue.append(tid)

    @staticmethod
    def _refresh_workflow_status(conn: sqlite3.Connection, workflow_id: str,
                                 now: float) -> None:
        wf = conn.execute(
            "SELECT status FROM workflows WHERE workflow_id = ?", (workflow_id,)
        ).fetchone()
        if wf is None or wf["status"] == WF_CANCELLED:
            return
        rows = conn.execute(
            "SELECT status, COUNT(*) AS c FROM tasks WHERE workflow_id = ?"
            " GROUP BY status",
            (workflow_id,),
        ).fetchall()
        counts = {r["status"]: r["c"] for r in rows}
        active = sum(counts.get(s, 0) for s in (PENDING, READY, RUNNING))
        if active > 0:
            status = WF_RUNNING
        elif counts.get(FAILED, 0) or counts.get(SKIPPED, 0):
            status = WF_FAILED
        else:
            status = WF_SUCCEEDED
        if status != wf["status"]:
            conn.execute(
                "UPDATE workflows SET status = ?, updated_at = ? WHERE workflow_id = ?",
                (status, now, workflow_id),
            )

    @staticmethod
    def _workflow_view(conn: sqlite3.Connection, workflow_id: str) -> dict:
        wf = conn.execute(
            "SELECT * FROM workflows WHERE workflow_id = ?", (workflow_id,)
        ).fetchone()
        task_rows = conn.execute(
            "SELECT * FROM tasks WHERE workflow_id = ? ORDER BY created_at, task_id",
            (workflow_id,),
        ).fetchall()
        dep_rows = conn.execute(
            "SELECT task_id, depends_on FROM task_deps WHERE workflow_id = ?"
            " ORDER BY depends_on",
            (workflow_id,),
        ).fetchall()
        deps: dict[str, list[str]] = {}
        for d in dep_rows:
            deps.setdefault(d["task_id"], []).append(d["depends_on"])
        return {
            "workflow_id": wf["workflow_id"],
            "idempotency_key": wf["idempotency_key"],
            "status": wf["status"],
            "created_at": _iso(wf["created_at"]),
            "updated_at": _iso(wf["updated_at"]),
            "tasks": [
                {
                    "task_id": t["task_id"],
                    "status": t["status"],
                    "depends_on": deps.get(t["task_id"], []),
                    "attempts": t["attempts"],
                    "max_attempts": t["max_attempts"],
                    "lease_expires_at": _iso(t["lease_expires_at"]),
                }
                for t in task_rows
            ],
        }
