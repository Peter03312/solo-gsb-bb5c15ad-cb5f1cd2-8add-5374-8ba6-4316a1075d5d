# 批处理任务编排服务（Python + FastAPI + SQLite）

外部工作进程通过 HTTP 领取并执行任务的批处理编排服务。支持以 DAG 描述工作流、
幂等提交、租约令牌、失败重试、租约到期回收、失败级联跳过、取消与状态查询；
所有状态保存在 SQLite，**进程/容器重启后状态和租约继续有效**。

## 1. 一键启动（Docker Compose）

```bash
docker compose up -d --build
```

启动时自动完成数据库建表初始化（幂等），数据保存在命名卷 `orchestrator-data`
（容器内 `/data/orchestrator.db`）。健康检查：

```bash
curl http://127.0.0.1:8000/health
```

服务地址与端口：

| 项目 | 地址 |
| --- | --- |
| API Base URL | `http://127.0.0.1:8000` |
| Swagger 交互文档 | `http://127.0.0.1:8000/docs` |
| OpenAPI Schema | `http://127.0.0.1:8000/openapi.json` |
| 健康检查 | `GET http://127.0.0.1:8000/health` |

停止 / 清理：

```bash
docker compose logs -f          # 查看日志
docker compose down             # 停止（保留数据卷）
docker compose down -v          # 停止并删除数据卷
```

## 2. 本地直接运行（不用 Docker）

需要 Python 3.11+：

```bash
pip install -r requirements.txt
# 数据库目录需可写；不设置则默认 /data/orchestrator.db
export ORCH_DB_PATH=./data/orchestrator.db
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 3. 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ORCH_DB_PATH` | `/data/orchestrator.db` | SQLite 数据库文件路径 |
| `ORCH_LEASE_SECONDS` | `30` | 领取任务时租约默认时长（秒） |
| `ORCH_DEFAULT_MAX_ATTEMPTS` | `3` | 工作流默认最大尝试次数 |
| `ORCH_REAPER_INTERVAL_SECONDS` | `10` | 后台扫描到期租约的间隔（秒）；`0` 关闭后台线程 |
| `ORCH_BUSY_TIMEOUT_MS` | `5000` | SQLite 写锁忙等待毫秒数 |

单次领取也可以在请求体中传 `lease_seconds`（1~3600）覆盖默认租约时长。

## 4. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 提交工作流（幂等） |
| GET | `/workflows` | 列出工作流（`limit`/`offset`） |
| GET | `/workflows/{id}` | 查询工作流及全部任务状态 |
| POST | `/workflows/{id}/cancel` | 取消工作流 |
| POST | `/workflows/{id}/claim` | 在指定工作流领取一个就绪任务 |
| POST | `/claims/claim` | 跨所有工作流领取一个就绪任务 |
| POST | `/tasks/{task_id}/succeed` | 持令牌报告成功 |
| POST | `/tasks/{task_id}/fail` | 持令牌报告失败 |

任务状态：`pending`（待领取）、`leased`（已租约/执行中）、`succeeded`、
`failed`、`skipped`（因上游失败被跳过）、`cancelled`。

工作流状态：`pending`、`running`、`succeeded`、`failed`、`cancelled`。

### 语义约定

- **任务图不可变**：接受后没有任何修改接口；重复提交仅返回原工作流。
- **受理校验（HTTP 422）**：缺失依赖、任务标识重复、依赖成环（含自依赖）。
- **幂等**：同一 `idempotency_key` + 相同请求返回原工作流（`replayed=true`）；
  同一幂等键 + 不同请求返回 `409 idempotency_conflict`。
- **领取**：只有全部依赖均为 `succeeded` 的任务可领取；每次领取
  `attempts + 1`，返回一次性 `token` 与 `leased_until` 租约截止时间。
  并发领取通过数据库条件更新串行化，**同一任务不会被重复分配**。
- **上报**：只有持有与当前租约匹配的令牌才能成功/失败上报；旧令牌、
  租约到期后被重领的旧令牌、取消工作流的旧令牌一律 `403 invalid_token`。
- **失败/到期**：未达 `max_attempts` 回到 `pending` 可重领；达到上限置
  `failed`，其所有未完成后继递归置 `skipped`，其他分支继续执行。
- **取消**：工作流置 `cancelled`，所有未完成（`pending`/`leased`）任务置
  `cancelled` 且租约令牌失效；已成功的任务保持成功。对终态工作流取消返回
  `409 workflow_terminal`，对已取消工作流重复取消幂等成功。
- **持久化**：状态、尝试计数、令牌哈希、租约截止时间全部落 SQLite
  （WAL 模式），重启后保持；租约到期判断以落库的绝对截止时间为准。

## 5. 调用示例

### 5.1 提交工作流（curl）

```bash
curl -s -X POST http://127.0.0.1:8000/workflows \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotency_key": "wf-20261006-001",
    "max_attempts": 3,
    "tasks": [
      {"key": "extract",   "dependencies": []},
      {"key": "transform", "dependencies": ["extract"], "max_attempts": 5},
      {"key": "validate",  "dependencies": ["extract"]},
      {"key": "load",      "dependencies": ["transform", "validate"]}
    ]
  }'
```

响应 `201`：

```json
{
  "id": "7292fd7f6e7c4f3689f1175be8b2252c",
  "status": "pending",
  "idempotency_key": "wf-20261006-001",
  "default_max_attempts": 3,
  "replayed": false,
  "tasks": [ ... ]
}
```

字段说明：`max_attempts` 是工作流默认值；单个任务可用自身的 `max_attempts`
覆盖。任务标识 `key` 在工作流内必须唯一。

### 5.2 工作进程：领取 → 执行 → 上报

```bash
# 领取（就绪任务不存在时返回 204，工作进程应退避后重试）
LEASE=$(curl -s -X POST http://127.0.0.1:8000/workflows/<WORKFLOW_ID>/claim \
  -H 'Content-Type: application/json' -d '{"lease_seconds": 60}')
echo "$LEASE"
# {
#   "workflow_id": "...", "task_id": "...", "key": "extract",
#   "attempt": 1, "max_attempts": 3,
#   "token": "Pdp1cNE9...", "leased_until": "2026-10-06T04:32:06+00:00",
#   "deadline_seconds": 60
# }

TASK_ID=$(... 从 $LEASE 解析 task_id)
TOKEN=$(... 从 $LEASE 解析 token)

# 成功
curl -s -X POST http://127.0.0.1:8000/tasks/$TASK_ID/succeed \
  -H 'Content-Type: application/json' \
  -d "{\"token\": \"$TOKEN\", \"result\": \"optional result text\"}"

# 失败（未达上限会重新可领取，达到上限级联跳过后继）
curl -s -X POST http://127.0.0.1:8000/tasks/$TASK_ID/fail \
  -H 'Content-Type: application/json' \
  -d "{\"token\": \"$TOKEN\", \"error\": \"division by zero\"}"
```

跨工作流通用工作进程只需把领取地址换成
`POST http://127.0.0.1:8000/claims/claim`，响应中带有 `workflow_id`。

### 5.3 查询与取消

```bash
curl -s http://127.0.0.1:8000/workflows/<WORKFLOW_ID>
curl -s -X POST http://127.0.0.1:8000/workflows/<WORKFLOW_ID>/cancel
```

### 5.4 Python 示例脚本（仅标准库）

```bash
python examples/client_demo.py          # 提交→幂等→冲突→重试→完成 全流程
python examples/worker.py               # 通用工作进程（轮询 /claims/claim）
```

## 6. 错误响应格式

所有业务错误统一为：

```json
{"error": {"code": "idempotency_conflict", "message": "...", ...}}
```

| HTTP | code |
| --- | --- |
| 422 | `missing_dependency` / `duplicate_task_key` / `dependency_cycle` |
| 409 | `idempotency_conflict` / `workflow_terminal` / `not_leased` |
| 403 | `invalid_token` |
| 404 | `workflow_not_found` / `task_not_found` |

## 7. 测试

```bash
pip install pytest httpx
python -m pytest -q
```

覆盖：图校验三类拒绝、幂等提交/冲突、依赖门控、令牌鉴权与一次性、
失败重试与上限级联、租约到期（懒回收 + 后台线程）、取消、
20 线程并发领取不重复分配、跨“重启”状态与租约保持。

## 8. 目录结构

```
app/
  config.py    # 环境变量配置
  db.py        # SQLite 连接、WAL、建表初始化
  models.py    # 请求/响应模型
  errors.py    # 统一业务错误
  service.py   # 核心业务逻辑（校验/幂等/领取/上报/级联/取消）
  main.py      # FastAPI 路由与后台租约回收
examples/      # 调用示例（client_demo.py、worker.py）
tests/         # pytest 测试
Dockerfile
docker-compose.yml
```
