"""Request/response models for the HTTP API."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class TaskSpec(BaseModel):
    task_id: str = Field(min_length=1, max_length=128)
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = Field(default=3, ge=1, le=1000)


class SubmitWorkflowRequest(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=256)
    tasks: list[TaskSpec] = Field(min_length=1, max_length=1000)


class ClaimRequest(BaseModel):
    task_id: Optional[str] = Field(default=None, max_length=128)
    lease_seconds: Optional[int] = Field(default=None, ge=1)


class ResultRequest(BaseModel):
    lease_token: str = Field(min_length=1, max_length=128)
    outcome: Literal["success", "failure"]


class TaskView(BaseModel):
    task_id: str
    status: str
    depends_on: list[str]
    attempts: int
    max_attempts: int
    lease_expires_at: Optional[str]


class WorkflowView(BaseModel):
    workflow_id: str
    idempotency_key: str
    status: str
    created_at: str
    updated_at: str
    tasks: list[TaskView]


class WorkflowSummary(BaseModel):
    workflow_id: str
    idempotency_key: str
    status: str
    created_at: str
    updated_at: str


class ClaimView(BaseModel):
    workflow_id: str
    task_id: str
    attempt: int
    lease_token: str
    lease_expires_at: str
