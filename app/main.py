"""FastAPI 入口：HTTP API、统一错误处理、后台租约回收。"""
from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import Depends, FastAPI, Query, status
from fastapi.responses import JSONResponse, Response

from . import service
from .db import connect, get_conn, get_settings, init_db
from .errors import ServiceError
from .models import (
    ActionOut,
    CancelOut,
    LeaseIn,
    LeaseOut,
    ReportIn,
    WorkflowCreatedOut,
    WorkflowIn,
    WorkflowOut,
)


def create_app() -> FastAPI:
    settings = get_settings()
    init_db(settings)

    stop_event = threading.Event()

    def reaper_loop() -> None:
        interval = settings.reaper_interval_seconds
        if interval <= 0:
            return
        while not stop_event.wait(interval):
            try:
                conn = connect(settings)
                try:
                    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    with service.transaction(conn):
                        affected = service.expire_leases(conn, now)
                        for wf_id in affected:
                            service.finalize_workflow(conn, wf_id, now)
                finally:
                    conn.close()
            except Exception:  # noqa: BLE001 - 后台线程不能因单次异常退出
                pass

    reaper_thread = threading.Thread(
        target=reaper_loop, name="lease-reaper", daemon=True
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> Iterator[None]:
        reaper_thread.start()
        try:
            yield
        finally:
            stop_event.set()

    app = FastAPI(
        title="批处理任务编排服务",
        version="1.0.0",
        description="提交 DAG 工作流，工作进程凭租约令牌领取并上报任务。",
        lifespan=lifespan,
    )

    @app.exception_handler(ServiceError)
    async def handle_service_error(_request, exc: ServiceError):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    **exc.details,
                }
            },
        )
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    # ------------------------------------------------------------------ #
    # 健康检查
    # ------------------------------------------------------------------ #
    @app.get("/health", tags=["system"])
    def health(conn=Depends(get_conn)):
        conn.execute("SELECT 1").fetchone()
        return {"status": "ok"}

    # ------------------------------------------------------------------ #
    # 工作流
    # ------------------------------------------------------------------ #
    @app.post(
        "/workflows",
        response_model=WorkflowCreatedOut,
        status_code=status.HTTP_201_CREATED,
        tags=["workflows"],
    )
    def submit_workflow(payload: WorkflowIn, conn=Depends(get_conn)):
        detail, replayed = service.create_workflow(conn, payload, settings)
        detail["replayed"] = replayed
        return detail

    @app.get(
        "/workflows", response_model=list[WorkflowOut], tags=["workflows"]
    )
    def list_all_workflows(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        conn=Depends(get_conn),
    ):
        return service.list_workflows(conn, limit, offset)

    @app.get(
        "/workflows/{workflow_id}",
        response_model=WorkflowOut,
        tags=["workflows"],
    )
    def get_workflow(workflow_id: str, conn=Depends(get_conn)):
        return service.get_workflow_detail(conn, workflow_id, now_iso())

    @app.post(
        "/workflows/{workflow_id}/cancel",
        response_model=CancelOut,
        tags=["workflows"],
    )
    def cancel_workflow(workflow_id: str, conn=Depends(get_conn)):
        return service.cancel_workflow(conn, workflow_id, now_iso())

    # ------------------------------------------------------------------ #
    # 领取 / 上报
    # ------------------------------------------------------------------ #
    @app.post(
        "/workflows/{workflow_id}/claim",
        response_model=LeaseOut,
        responses={204: {"description": "当前没有可领取的任务"}},
        tags=["tasks"],
    )
    def claim_in_workflow(
        workflow_id: str, body: LeaseIn | None = None, conn=Depends(get_conn)
    ):
        lease_seconds = body.lease_seconds if body else None
        claimed = service.claim_task(
            conn,
            settings,
            now_iso(),
            lease_seconds=lease_seconds,
            workflow_id=workflow_id,
        )
        if claimed is None:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        return claimed

    @app.post(
        "/claims/claim",
        response_model=LeaseOut,
        responses={204: {"description": "当前没有可领取的任务"}},
        tags=["tasks"],
    )
    def claim_any(body: LeaseIn | None = None, conn=Depends(get_conn)):
        """从所有未终结工作流中领取一个就绪任务（供通用工作进程轮询）。"""
        lease_seconds = body.lease_seconds if body else None
        claimed = service.claim_task(
            conn, settings, now_iso(), lease_seconds=lease_seconds
        )
        if claimed is None:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        return claimed

    @app.post(
        "/tasks/{task_id}/succeed",
        response_model=ActionOut,
        tags=["tasks"],
    )
    def task_succeed(
        task_id: str, body: ReportIn, conn=Depends(get_conn)
    ):
        return service.report_task(
            conn,
            settings,
            task_id,
            body.token,
            success=True,
            error=None,
            result=body.result,
            now=now_iso(),
        )

    @app.post(
        "/tasks/{task_id}/fail",
        response_model=ActionOut,
        tags=["tasks"],
    )
    def task_fail(task_id: str, body: ReportIn, conn=Depends(get_conn)):
        return service.report_task(
            conn,
            settings,
            task_id,
            body.token,
            success=False,
            error=body.error,
            result=None,
            now=now_iso(),
        )

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
    )
