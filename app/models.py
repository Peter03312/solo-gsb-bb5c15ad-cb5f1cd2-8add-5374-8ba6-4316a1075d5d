"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

TaskStatus = Literal[
    "pending", "leased", "succeeded", "failed", "skipped", "cancelled"
]
WorkflowStatus = Literal[
    "pending", "running", "succeeded", "failed", "cancelled"
]


class TaskIn(BaseModel):
    key: str = Field(..., min_length=1, description="工作流内唯一任务标识")
    dependencies: list[str] = Field(
        default_factory=list, description="依赖的任务标识列表"
    )
    max_attempts: int | None = Field(
        default=None, ge=1, description="该任务最大尝试次数，缺省用工作流默认值"
    )


class WorkflowIn(BaseModel):
    tasks: list[TaskIn] = Field(..., min_length=1)
    idempotency_key: str = Field(..., min_length=1)
    max_attempts: int = Field(
        default=3, ge=1, description="工作流默认最大尝试次数"
    )


class ReportIn(BaseModel):
    token: str = Field(..., min_length=1)
    error: str | None = Field(
        default=None, max_length=4000, description="失败原因（失败时建议填写）"
    )
    result: str | None = Field(
        default=None, max_length=4000, description="成功结果（可选，原样保存）"
    )


class LeaseIn(BaseModel):
    worker_id: str | None = Field(
        default=None,
        max_length=200,
        description="工作进程标识，仅用于可观测性（记录在结果中可选）",
    )
    lease_seconds: int | None = Field(
        default=None, ge=1, le=3600, description="本次租约时长，缺省用服务默认值"
    )


class TaskOut(BaseModel):
    id: str
    key: str
    status: TaskStatus
    dependencies: list[str]
    attempts: int
    max_attempts: int
    leased_until: str | None
    result: str | None
    error: str | None
    created_at: str
    updated_at: str


class WorkflowOut(BaseModel):
    id: str
    status: WorkflowStatus
    idempotency_key: str
    default_max_attempts: int
    created_at: str
    updated_at: str
    tasks: list[TaskOut]


class ReplayedMixin(BaseModel):
    replayed: bool = Field(..., description="True 表示命中幂等键返回的既有工作流")


class WorkflowCreatedOut(WorkflowOut, ReplayedMixin):
    pass


class LeaseOut(BaseModel):
    workflow_id: str
    task_id: str
    key: str
    attempt: int = Field(..., description="本次是第几次尝试，从 1 开始")
    max_attempts: int
    token: str
    leased_until: str
    deadline_seconds: int


class ActionOut(BaseModel):
    workflow_id: str
    task_id: str
    status: TaskStatus
    attempts: int


class CancelOut(BaseModel):
    id: str
    status: WorkflowStatus
    cancelled_tasks: int
    updated_at: str


class ErrorOut(BaseModel):
    error: dict
