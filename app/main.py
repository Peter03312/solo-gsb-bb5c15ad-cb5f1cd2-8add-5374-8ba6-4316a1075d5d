"""HTTP API for the batch job orchestrator."""
from __future__ import annotations

from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse

from .config import Settings
from .db import Database
from .schemas import (
    ClaimRequest,
    ClaimView,
    ResultRequest,
    SubmitWorkflowRequest,
    WorkflowSummary,
    WorkflowView,
)
from .service import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    ValidationError,
    WorkflowService,
)

ERROR_CODES = {
    ValidationError: status.HTTP_400_BAD_REQUEST,
    ForbiddenError: status.HTTP_403_FORBIDDEN,
    NotFoundError: status.HTTP_404_NOT_FOUND,
    ConflictError: status.HTTP_409_CONFLICT,
}


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    service = WorkflowService(
        Database(settings.database_path),
        default_lease_seconds=settings.default_lease_seconds,
        max_lease_seconds=settings.max_lease_seconds,
    )

    app = FastAPI(title="Batch Job Orchestrator", version="1.0.0")
    app.state.service = service

    for exc_type, code in ERROR_CODES.items():
        @app.exception_handler(exc_type)
        async def service_error_handler(request: Request, exc: Exception,
                                        _code: int = code) -> JSONResponse:
            return JSONResponse(status_code=_code, content={"detail": str(exc)})

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/workflows", response_model=WorkflowView,
              status_code=status.HTTP_201_CREATED)
    def submit_workflow(payload: SubmitWorkflowRequest, response: Response) -> dict:
        view, created = service.submit_workflow(payload)
        if not created:
            # Idempotent replay: same key + same request -> the original workflow.
            response.status_code = status.HTTP_200_OK
        return view

    @app.get("/workflows", response_model=list[WorkflowSummary])
    def list_workflows() -> list[dict]:
        return service.list_workflows()

    @app.get("/workflows/{workflow_id}", response_model=WorkflowView)
    def get_workflow(workflow_id: str) -> dict:
        return service.get_workflow(workflow_id)

    @app.post("/workflows/{workflow_id}/cancel", response_model=WorkflowView)
    def cancel_workflow(workflow_id: str) -> dict:
        return service.cancel_workflow(workflow_id)

    @app.post("/workflows/{workflow_id}/claim", response_model=ClaimView)
    def claim(workflow_id: str, payload: ClaimRequest | None = None) -> dict:
        payload = payload or ClaimRequest()
        return service.claim(workflow_id, payload.task_id, payload.lease_seconds)

    @app.post("/claims", response_model=ClaimView)
    def claim_any(payload: ClaimRequest | None = None) -> dict:
        payload = payload or ClaimRequest()
        return service.claim_any(payload.lease_seconds)

    @app.post("/workflows/{workflow_id}/tasks/{task_id}/result",
              response_model=WorkflowView)
    def report_result(workflow_id: str, task_id: str, payload: ResultRequest) -> dict:
        return service.report_result(
            workflow_id, task_id, payload.lease_token, payload.outcome
        )

    return app


app = create_app()
