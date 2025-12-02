# DistributedTasks（分布式任务平台）

一个基于 Go 的分布式任务平台，支持即时任务入队、Worker 消费执行、失败重试、延时队列与死信队列（DLQ）管理。以"可靠的最小闭环"为目标，逐步演进到可生产使用。

## 技术栈

- 语言与构建：Go 1.25.0，Go Modules
- Web 框架：Gin
- 数据库：PostgreSQL（任务及执行记录持久化）
- 队列与缓存：Redis v9（List + ZSET，RPUSH + BLPOP）
- 调度/部署（规划）：cron、Docker、Nginx、Supervisor

## 目录结构

```
.
├── cmd/
│   ├── api/
│   │   └── main.go              # API 入口
│   └── worker/
│       └── main.go              # Worker 入口
├── internal/
│   ├── config/config.go         # 配置加载
│   ├── db/postgres.go           # DB 初始化与 EnsureSchema
│   ├── domain/                  # 领域模型
│   │   ├── task.go
│   │   └── taskrun.go
│   ├── http/handler/task_handler.go
│   ├── queue/redis.go           # 队列操作（ready/delayed/dlq/锁/原子搬运）
│   ├── repo/                    # 数据访问层
│   │   ├── task_repo.go
│   │   └── taskrun_repo.go
│   └── service/task_service.go  # 业务编排
├── 技术文档/
│   ├── 从0到1构建流程.md
│   └── 分布式任务平台技术方案.md
├── go.mod
├── go.sum
└── main.go
```

## 核心模型与执行链路

### 模型设计

- **Task**（任务总体）：队列名、优先级、最大重试、幂等键、payload、status（queued/completed/failed）
- **TaskRun**（一次尝试）：attempt、status（queued/retrying/succeeded/failed）、next_retry_at、result

### 执行链路（即时任务）

1. **API 创建任务** → DB 写 Task(queued) + TaskRun(attempt=1, queued)
2. **消息入队** → ready 队列（FIFO：RPUSH），Worker BLPOP 消费
3. **Worker 执行**：
   - **成功**：TaskRun → succeeded；Task → completed
   - **失败可重试**：TaskRun → retrying（记录 next_retry_at 与失败原因），新建 TaskRun(attempt+1, queued)，消息入 delayed（ZSET）
   - **达最大重试**：TaskRun → failed；Task → failed，消息入 DLQ（List）
4. **搬运器**：定时扫描 delayed，原子搬运（Lua 脚本）到 ready，并用分布式锁（SETNX+EX）防并发

**说明**：进入 DLQ 后不会自动改变优先级；仅在"重放接口"传入 `override_priority` 时才覆盖（例如奇数改偶数）以避免再次失败。

## 环境变量

- `HTTP_PORT`：默认 8080
- `DATABASE_URL`：如 `host=localhost port=5432 user=linhe dbname=task_platform sslmode=disable`
- `REDIS_URL`：如 `redis://localhost:6379`
- `QUEUE_NAMES`：订阅队列列表，如 `default`
- `WORKER_CONCURRENCY`：默认 1

## 快速开始

### 启动服务

1. 启动 API
   ```bash
   go run ./cmd/api
   ```

2. 启动 Worker（订阅 default 队列）
   ```bash
   QUEUE_NAMES=default go run ./cmd/worker
   ```

### 健康检查

- `GET /healthz` → 200
- `GET /readyz` → 200，返回 `{"ready":true}`（同时检查 DB/Redis）

## API 使用指南

### 1. 创建即时任务（直接成功）

**请求**：
```bash
POST /api/v1/tasks
Content-Type: application/json

{
  "name": "demo-even",
  "type": "immediate",
  "queue_name": "default",
  "priority": 10,
  "payload": {"k":"v"},
  "dedup_key": "demo-even-001",
  "max_retries": 3
}
```

**响应**：
```json
{
  "task_id": "uuid",
  "status": "queued"
}
```

**验证**：
```bash
GET /api/v1/tasks/{id}
```

预期：`task.status=completed`，`latest_run.status=succeeded`

### 2. 创建任务（失败后重试成功）

**请求**：
```bash
POST /api/v1/tasks
Content-Type: application/json

{
  "name": "demo-odd-retry",
  "type": "immediate",
  "queue_name": "default",
  "priority": 11,
  "payload": {"k":"v"},
  "dedup_key": "demo-odd-retry-001",
  "max_retries": 3
}
```

**验证流程**：
1. 立即查询：`GET /api/v1/tasks/{id}`
   - 预期：`latest_run.status=retrying`，`next_retry_at` 有值
2. 等待 2-5 秒（搬运器执行）
3. 再次查询：`GET /api/v1/tasks/{id}`
   - 预期：`latest_run.status=succeeded`，`task.status=completed`

### 3. 查看任务详情

**请求**：
```bash
GET /api/v1/tasks/{id}
```

**响应**：
```json
{
  "task": {
    "id": "uuid",
    "name": "demo",
    "status": "completed",
    ...
  },
  "latest_run": {
    "attempt": 2,
    "status": "succeeded",
    "next_retry_at": null,
    ...
  }
}
```

### 4. DLQ 管理

#### 查看死信队列

```bash
GET /api/v1/queues/default/dlq?count=50
```

#### 重放死信（支持覆盖优先级）

**请求**：
```bash
POST /api/v1/queues/default/dlq/replay
Content-Type: application/json

{
  "count": 1,
  "override_priority": 10
}
```

**说明**：只有传入 `override_priority` 才会覆盖优先级；默认不自动修改。

## 阶段进度

- ✅ **阶段 0**：Gin 服务、配置、DB/Redis 连接与健康检查
- ✅ **阶段 1**：Task/TaskRun 模型、表结构与仓库
- ✅ **阶段 2**：创建/查询任务 API
- ✅ **阶段 3**：即时任务入队 + Worker 消费 + 状态回写（基础版）
- ✅ **阶段 4**：失败重试 + 延时队列 + DLQ（可用版）
  - FIFO 队列（RPUSH + BLPOP）
  - 原子搬运（Lua 脚本）
  - 分布式锁（防并发搬运）
  - DLQ 重放支持覆盖优先级

## 常见问题排查

### Worker 未订阅 default 队列
- **现象**：任务创建后一直 queued，Worker 无日志
- **排查**：Worker 启动日志需显示 `queues=[default]`
- **解决**：启动时设置 `QUEUE_NAMES=default`

### 201 状态码未显示
- **现象**：不确定返回的状态码
- **排查**：Postman 顶部 Status 才是状态码；Body 显示 JSON
- **解决**：用 `curl -i` 查看完整响应头

### task_runs.updated_at 列不存在
- **现象**：更新 TaskRun 时报错
- **排查**：检查表结构
- **解决**：EnsureSchema 会自动补列或手动执行：
  ```sql
  ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
  ```

### 重放后仍失败
- **现象**：DLQ 重放后任务再次进入 DLQ
- **原因**：奇数优先级 + attempt==1 触发模拟失败
- **解决**：使用 `override_priority`（如 10），或后续扩展 payload 的 `skip_fail=true`

## 后续规划

### 阶段 5：租约与心跳
- Worker 并发执行与租约机制
- Worker 注册与心跳上报
- 租约过期自动接管

### 阶段 6：定时调度
- 基于 cron 的 Scheduler
- 定时生成 TaskRun 并入队
- 补偿机制

### 阶段 7：部署落地
- Docker 镜像
- docker-compose 集成
- Nginx 反向代理
- Supervisor 保活 Worker

### 阶段 8：监控与告警
- 结构化日志
- Prometheus 指标
- 队列长度/失败率告警

### 阶段 9：安全治理
- API Key / JWT 认证
- 速率限制
- 审计日志

### 阶段 10：迭代优化
- 性能优化（批量更新、连接池、Lua 脚本优化）
- 多租户支持
- 任务编排（DAG）
- Web 控制台

## 贡献指南

欢迎提交 PR/Issue：
- 修复 bug
- 改进重试策略、搬运原子性
- 增强 DLQ 重放能力
- Worker 并发与心跳
- 提交信息建议采用 Conventional Commits 规范

## 许可证

MIT License
