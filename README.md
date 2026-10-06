# Batch Job Orchestrator

基于 **Python + FastAPI + SQLite** 的批处理编排服务。外部工作进程通过 HTTP API
领取任务、按租约上报结果；服务负责依赖调度、重试、失败传播与取消，所有状态
（含未到期租约）持久化在 SQLite 中，重启后不丢失。

## 快速启动

### Docker Compose（一键启动，自动完成建表初始化）

```bash
docker compose up --build
```

服务地址：**http://localhost:8000**，交互式 API 文档：**http://localhost:8000/docs**。
SQLite 数据文件保存在命名卷 `orchestrator-data` 中，容器重建/重启后状态保留。

### 本地开发

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试与演示

```bash
pytest tests/            # 22 个端到端测试
./scripts/demo.sh        # 完整调用示例（需服务已启动）
```

## 配置

均通过环境变量配置（`docker-compose.yml` 中已给出默认值）：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DATABASE_PATH` | `/data/orchestrator.db`（容器）/ `./data/orchestrator.db`（本地） | SQLite 文件路径 |
| `DEFAULT_LEASE_SECONDS` | `30` | 领取任务时的默认租约时长（秒） |
| `MAX_LEASE_SECONDS` | `3600` | 租约时长上限（秒） |

## 核心语义

- **提交即不可变**：工作流被接受后任务图不可修改。缺失依赖、重复 `task_id`、
  依赖成环（含自环）在提交时拒绝（400）。
- **幂等提交**：同一 `idempotency_key` + 相同请求体 → 返回原工作流（200）；
  同一 key 不同请求体 → 409 冲突。
- **领取（claim）**：只有所有依赖都 `SUCCEEDED` 的任务（`READY`）可被领取。
  每次领取计一次尝试（`attempts + 1`），返回有期限的租约和一次性令牌。
  领取在 `BEGIN IMMEDIATE` 事务内以条件 UPDATE 完成，并发领取不会重复分配。
- **上报**：仅持有当前有效令牌且租约未到期者可上报成功/失败；
  令牌不匹配 403，任务不在运行态 409。
- **重试与失败传播**：失败或租约到期后，未达 `max_attempts` 的任务回到
  `READY` 可重领；达到上限则 `FAILED`，其全部未完成后继被级联 `SKIPPED`，
  其他分支不受影响继续调度。
- **取消**：取消后所有未完成任务转为 `CANCELLED`，禁止领取（409），
  旧令牌上报被拒绝（409）。取消幂等；已终结（SUCCEEDED/FAILED）的工作流
  不可取消。
- **持久化**：工作流、任务、尝试次数、租约令牌与到期时间均存于 SQLite，
  进程重启后状态与租约保持不变；到期租约在下次操作时惰性回收。

### 状态机

任务：`PENDING → READY → RUNNING → SUCCEEDED`，
`RUNNING → READY`（失败/租约到期且可重试），`RUNNING → FAILED`（尝试耗尽），
`PENDING/READY → SKIPPED`（祖先失败），`PENDING/READY/RUNNING → CANCELLED`。

工作流：`RUNNING → SUCCEEDED`（全部成功）/ `FAILED`（存在 FAILED/SKIPPED
且无在途任务）/ `CANCELLED`。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 健康检查 |
| POST | `/workflows` | 提交工作流（201 新建 / 200 幂等重放 / 409 冲突） |
| GET | `/workflows` | 列出全部工作流 |
| GET | `/workflows/{id}` | 查询工作流及任务状态 |
| POST | `/workflows/{id}/cancel` | 取消工作流 |
| POST | `/workflows/{id}/claim` | 领取任务（可指定 `task_id` 与 `lease_seconds`） |
| POST | `/claims` | 跨工作流领取最老的可领取任务 |
| POST | `/workflows/{id}/tasks/{task_id}/result` | 上报成功/失败 |

## 调用示例

```bash
BASE=http://localhost:8000

# 1. 提交工作流：extract -> transform -> load，audit 为独立分支
curl -s -X POST $BASE/workflows -H 'Content-Type: application/json' -d '{
  "idempotency_key": "etl-2026-10-06",
  "tasks": [
    {"task_id": "extract",   "max_attempts": 3},
    {"task_id": "transform", "depends_on": ["extract"],   "max_attempts": 2},
    {"task_id": "load",      "depends_on": ["transform"], "max_attempts": 1},
    {"task_id": "audit",     "max_attempts": 1}
  ]}'
# => 201 {"workflow_id": "8f3d...", "status": "RUNNING", "tasks": [...]}

# 2. 工作进程领取任务（获得租约与令牌）
curl -s -X POST $BASE/workflows/8f3d.../claim \
  -H 'Content-Type: application/json' -d '{"lease_seconds": 60}'
# => {"task_id": "extract", "attempt": 1,
#     "lease_token": "c41a...", "lease_expires_at": "2026-10-06T12:01:00+00:00"}

# 3. 持令牌上报结果（success / failure）
curl -s -X POST $BASE/workflows/8f3d.../tasks/extract/result \
  -H 'Content-Type: application/json' \
  -d '{"lease_token": "c41a...", "outcome": "success"}'

# 4. 查询状态
curl -s $BASE/workflows/8f3d...

# 5. 取消工作流
curl -s -X POST $BASE/workflows/8f3d.../cancel
```

## 错误码

| 状态码 | 场景 |
|---|---|
| 400 | 图非法：重复标识、缺失依赖、成环 |
| 403 | 上报令牌与当前租约不匹配 |
| 404 | 工作流/任务不存在，或暂无可领取任务 |
| 409 | 幂等键冲突、任务不可领取、工作流已取消/终结、租约已失效 |
| 422 | 请求体不符合 schema |

## 项目结构

```
app/
  config.py    环境变量配置
  db.py        SQLite 连接与事务（WAL、BEGIN IMMEDIATE）
  schemas.py   请求/响应模型
  service.py   编排核心：校验、幂等、领取、租约、重试、跳过、取消
  main.py      FastAPI 路由与错误映射
tests/         pytest 端到端测试（含并发领取与重启持久化）
scripts/demo.sh 端到端调用示例
Dockerfile / docker-compose.yml
```
