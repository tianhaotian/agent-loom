# Agent Loom MVP

## 交付范围

当前 MVP 是一个 PostgreSQL 单 Provider、单开发 Tenant 的可运行垂直切片，目标是验证 Agent Loom 的持久化执行语义能够组成完整服务，而不是提供生产级多租户控制面。

已包含：

- 启动时自动执行 `0000` 至 `0018` migration；
- PostgreSQL 连接池与对象安全的 `DurableStore`；
- API Key 认证边界和 tenant-scoped 查询/命令；
- Workflow、Run、Stage、Artifact、Pending Action 和 Event HTTP 查询；
- 可从 `Last-Event-ID` 或 Event sequence 断点续读的 SSE；
- 八个必需 Stage，以及集成测试失败后原子创建的三阶段 attempt 2 返工链；
- 部署审批 Wait、重复事件幂等消费和持久化恢复 Task；
- 通过正式 Adapter/Execution 契约执行的 Mock Agent Server 与 Mock DevOps Tool；
- 可通过配置切换的真实 HTTP Agent Server 与 DevOps Tool profile；
- Scheduler、Recovery Worker、Agent Event/Stop/Status Worker、Transactional Outbox Publisher、过期 Lease 回收和 deadline/Wait/stale execution 维护服务；
- 幂等命令、Run version 和 execution generation fencing；
- JSON 结构化请求/Worker 日志和响应关联 ID；
- 从 HTTP 创建到 PostgreSQL 终态、返工、审批、Tool 部署和 deadline 的自动化 E2E 验收。

创建 Run 的生产路径会读取默认 `published` Workflow Version 3 中的 `agent-loom.execution-plan/v1`，验证 Task/Stage/Handler 引用后，在现有 PostgreSQL `CreateRun` 事务中原子实例化初始 PlanRevision、Checkpoint、Stage、Task 和 Dependency。初始、动态后继和 Wait 恢复 Task 都携带 `agent-loom.task-input/v1` 信封。通用 Workflow Worker从已验证注册表汇总可领取 kind，并根据稳定 Handler key 分发 Lease、输入和 Run fence；delivery 是当前首个真实 Handler，固定八阶段逻辑不再参与通用领取/路由。该 Plan Profile 能表达多个初始 Task、不使用业务 Stage 的 Agent Run，以及 `all`/`any` JoinPolicy 与成功状态或结果 JSON Pointer 条件；有依赖的 Task 在条件满足前保持 `scheduled`，完成前置 Task 的同一事务只会激活一次。当前 MVP 仍只开放默认 delivery Workflow，数据库领取暂不能按 Handler key 分区，更多 Handler 和动态修订后的 Task 实例化尚未接入。升级时仍兼容已在途的旧版无信封 delivery Task。

MVP 暂不包含 MySQL 事务 Provider、生产 SSO/RBAC、真实外部 Agent/DevOps 服务、Child Join 的后台自动轮询和生产可观测平台。这些属于 Phase 2B/3 或生产化扩展，不影响当前 PostgreSQL MVP 的权威执行闭环。

## 配置

| 环境变量 | 必需 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `AGENT_LOOM_DATABASE_URL` | 是 | 无 | PostgreSQL 连接 URL |
| `AGENT_LOOM_BIND` | 否 | `127.0.0.1:8080` | HTTP 监听地址 |
| `AGENT_LOOM_TENANT_KEY` | 否 | `mvp-local` | 单 Tenant 稳定身份 |
| `AGENT_LOOM_API_KEY` | 是 | 无 | 16 至 255 字符的 HTTP Bearer/API Key |
| `AGENT_LOOM_POOL_SIZE` | 否 | `8` | PostgreSQL 连接池上限 |
| `AGENT_LOOM_AGENT_BASE_URL` | 否* | 无 | 真实 Agent Server 基础 URL |
| `AGENT_LOOM_AGENT_TOKEN` | 否* | 无 | Agent Server Bearer token |
| `AGENT_LOOM_DEVOPS_BASE_URL` | 否* | 无 | 真实 DevOps 服务基础 URL |
| `AGENT_LOOM_DEVOPS_TOKEN` | 否* | 无 | DevOps 服务 Bearer token |

带 `*` 的四项必须全部省略或全部设置。全部省略时使用本地 Mock；全部设置时注册真实 HTTP profile。生产 Endpoint 必须使用 HTTPS，明文 HTTP 仅允许 `localhost`、`127.0.0.0/8` 或 `::1`。

数据库用户需要创建 schema、表、索引和执行普通 DML 的权限。不要把测试 URL 指向生产数据库。

## 启动

```bash
AGENT_LOOM_DATABASE_URL='postgresql://agent_loom:agent_loom@127.0.0.1:5432/agent_loom' \
AGENT_LOOM_API_KEY='replace-with-at-least-16-characters' \
  cargo run -p agent-loom-server
```

健康检查：

```bash
curl -sS http://127.0.0.1:8080/healthz
```

## API

### 创建 Run

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H 'authorization: Bearer replace-with-at-least-16-characters' \
  -H 'content-type: application/json' \
  -H 'idempotency-key: delivery-001' \
  -d '{"input":{"goal":"实现并交付 MVP"}}'
```

相同 `Idempotency-Key` 和相同请求会返回首次创建的 Run；同一个 Key 对应不同请求会被拒绝。

请求可选携带 `parent_run_id` 和 `parent_task_id` 创建 Child Run；`parent_task_id` 必须属于指定父 Run，且父 Run 必须存在、同租户并且未终止。创建首个关联 Child Run 会把父 Task 切换为 `scheduled`，并在父 Run 原子追加 `run.child_created` Event/Outbox。`GET /v1/runs/PARENT_RUN_ID/children` 按稳定创建顺序返回直接子 Run。子 Run 终止后，`POST /v1/runs/PARENT_RUN_ID/child-joins/TASK_ID` 以 `all` 或 `any` 策略评估终态子 Run；条件满足时原子激活父 Task 并追加 `run.child_join_satisfied` Event/Outbox，未满足时幂等返回 `no_op`。

### 查询 Run 与 Event

```bash
curl -sS -H 'authorization: Bearer replace-with-at-least-16-characters' \
  http://127.0.0.1:8080/v1/runs/RUN_ID
curl -sS -H 'authorization: Bearer replace-with-at-least-16-characters' \
  'http://127.0.0.1:8080/v1/runs/RUN_ID/events?after=0&limit=100'
```

所有 `/v1` 请求都必须携带 `Authorization: Bearer ...` 或 `X-API-Key`。还可查询 `/stages`、`/artifacts`、`/pending-actions` 和 `/events/stream`；SSE 断线后使用 `Last-Event-ID` 恢复。

### 查询和提交 PlanRevision

`GET /v1/runs/RUN_ID/plan-revisions` 返回从 revision 1 开始的完整不可变历史。`POST` 使用完整 `agent-loom.execution-plan/v1` 快照，并要求 `Idempotency-Key` 与当前 `base_revision`；提交会用 Run version、execution generation 和 Plan revision 双重 fencing，原子追加 `run.plan_revised` Event、Outbox、新 revision 和新增 Task。V1 动态修订采用 append-only 约束：可以追加 Task 和修改不透明 extension，但不能删除或改写已有 Task/Stage；新增 Task 继续绑定创建 Run 时的原始 input，并可依赖既有或同批新增 Task。

### 查询和更新 Context

`GET /v1/runs/RUN_ID/context-snapshots` 返回从 revision 1 开始的不可变 ContextSnapshot 历史。`POST` 要求 `Idempotency-Key`、`base_revision`、`merge_strategy`（`replace` 或 RFC 7396 `merge_patch`）和通用 JSON `patch`。Store 同时 fence Run version、execution generation 与当前 Context revision，并在一个事务中写入 ContextPatch、新 Snapshot、父级 lineage、`run.context_patched` Event、Outbox 和 Run 当前 Context 投影。

ExecutionPlan Task 可声明 `context_projection` JSON Pointer 列表。每个 Task 创建时固定引用当时的 ContextSnapshot；`GET /v1/tasks/TASK_ID/context` 返回该不可变引用和投影结果。空列表表示完整 Context，非空列表返回以 Pointer 为键的投影视图，因此后续 Context Patch 不会改变已创建 Task 的输入视图。

### 暂停、恢复、取消

控制命令必须提供 `Idempotency-Key`：

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs/RUN_ID/pause \
  -H 'authorization: Bearer replace-with-at-least-16-characters' \
  -H 'content-type: application/json' \
  -H 'idempotency-key: pause-001' \
  -d '{"reason":"manual review"}'
```

将路径中的 `pause` 替换为 `resume` 或 `cancel` 即可执行对应操作。

Pause/Cancel 会把已经提交或正在提交的远端 Agent 执行持久化为 `stopping`。后台 Agent Stop Worker 使用稳定幂等身份调用对应 Adapter；即使取消发生在提交响应返回前，迟到的远端 Run、Session 和协议版本仍会保留并继续触发停止。停止已受理或结果不确定时进入 `reconciling`，并由持久化 `status_poll_at` 驱动 Status Worker 调用 `get_status` 直到获得终态；不支持停止时进入 `manual_review`，不会把“请求已受理”误记为远端已取消。

提交成功的远端 Agent 会立即获得持久化轮询时间。Agent Event Worker 调用 Adapter 的 `read_events` 并传入已提交 cursor；事件 receipt、原始 digest、本地 Event、cursor CAS 和下一次轮询时间在一个 PostgreSQL 事务内提交。相同 cursor 上的重复 Worker 调用复用 Command Receipt，重复远端事件复用确定性 receipt，因此重启和 at-least-once 读取不会重复推进。终态批次会把执行切换为 `reconciling`，Status Worker 核验最终状态后停止事件轮询。

当 `submit` 响应丢失或明确返回不确定时，执行保持为 `outcome_unknown`。持久化恢复 Task 提交 reconciliation intent 后，Dispatcher 先使用原始提交幂等键调用 `reconcile_submission`；查到远端执行就补录 Run/Session/协议版本，明确不存在才安全重提。可恢复查询错误不会被误判为“远端不存在”，配置或能力缺失则进入 `manual_review`。

所有 Event 写入都会在同一事务创建 `run.events` Outbox 消息。生产进程中的 Outbox Publisher 以短 Lease 发布结构化 JSON 日志；失败或进程崩溃不会丢失消息，Lease 到期后可由另一进程接管，旧 attempt 会被 fencing。该通道是 at-least-once，后续 Broker Publisher 和消费者必须以 `event_id` 幂等。

### 注入等待事件

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs/RUN_ID/events \
  -H 'authorization: Bearer replace-with-at-least-16-characters' \
  -H 'content-type: application/json' \
  -H 'idempotency-key: approval-001' \
  -d '{
    "event_type":"approval.granted",
    "match_key":"approval-token",
    "payload_schema_version":1,
    "payload":{"approved":true}
  }'
```

## 验收

不连接数据库的质量门禁：

```bash
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

真实 PostgreSQL 验收：

```bash
AGENT_LOOM_TEST_POSTGRES_URL='postgresql://...' cargo test \
  -p agent-loom-server \
  -p agent-loom-store-postgres \
  -p agent-loom-provider-conformance \
  --all-targets -- --test-threads=1
```

`agent-loom-server` 的数据库 E2E 会验证未认证请求被拒绝，执行八个必需 Stage、一次三阶段返工、部署审批和 DevOps Tool，再检查 SSE、Stage、Artifact、Workflow 和 deadline 终态。
