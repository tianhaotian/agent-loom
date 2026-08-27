# Agent Loom

Agent Loom 是面向复杂 Agent 团队协作的持久化流程运行时。首个场景覆盖需求分析、原型/PRD、技术方案、编码、自测、集成测试和 DevOps 部署。

## 当前工程边界

```text
crates/domain          共享 ID、值对象、状态与只读投影
crates/durable-store   DurableStore 接口、命令、结果、错误和 conformance 清单
crates/adapter-core    Agent Server / Tool Adapter 通用接口
crates/adapter-http    真实 HTTP Agent Server / DevOps Tool profile 与 conformance
crates/runtime         Scheduler/Worker/Adapter 的数据库无关编排与服务生命周期
crates/server          可运行的 PostgreSQL MVP、HTTP API 与 Mock 业产研 Worker
crates/provider-conformance  PostgreSQL/MySQL 共享迁移与行为契约测试
crates/store-postgres  PostgreSQL Provider 与物理迁移
crates/store-mysql     MySQL/InnoDB Provider 与物理迁移
```

Runtime 内部按职责组织：

```text
runtime/src/adapter/    Registry、调用上下文、恢复分发与结果回写
runtime/src/agent_control/  远端 Agent stop/status 扫描、幂等调度与轮询服务
runtime/src/recovery/   reconcile Task 领取与外部执行启动事务
runtime/src/scheduler/  due-work 扫描、确定性计划与原子应用
runtime/src/service/    有界轮询、退避、关闭信号与 Job 接线
```

`domain`、`durable-store` 与 `adapter-core` 继续保持零外部依赖；Provider crate 可以引入各自的数据库驱动和异步运行时，但驱动类型不得泄漏到共享领域契约。`adapter-http` 使用 HTTPS/loopback HTTP 执行真实远程 I/O，`server` 是 PostgreSQL 专用的产品装配层，不改变 Runtime 与 Store 共享契约的数据库无关性。

已实现 `0000_migration_meta` 至 `0012_agent_status_poll` 的 PostgreSQL/MySQL 对等迁移，覆盖定义身份、Agent Endpoint、Run、Event、CommandReceipt、Stage、Task、TaskAttempt、Checkpoint、Wait、Artifact、ToolExecution 与 AgentExecution，并持久化恢复远端 Agent 生命周期所需的协议版本和状态查询时间。`provider-conformance` 会校验逻辑迁移顺序、表归属、终态 Event 与 Checkpoint 归属约束、Task Lease、Wait 单次消费槽与恢复计划、Tool/Agent 重试时间、Agent 请求信封、Artifact 版本血缘、外部执行幂等与 Agent Event 去重；只依赖 `dyn DurableStore` 的黑盒行为现已覆盖 Lease 到期重试、多 Worker 同时领取、完成/取消终态竞争、Wait 单次消费，以及完成事务中途约束失败后的完整回滚。

PostgreSQL 已接入真实驱动执行层：migration executor 使用 SHA-256 physical checksum、session advisory lock、step journal 和逐批 schema introspection；`PostgresStore` 通过连接池完整实现对象安全的 `DurableStore`，事务垂直切片已覆盖 Run 创建/查询、Event 分页、Task 生命周期、Wait 事件应用、ToolExecution 准备/结果记录、AgentExecution 提交/事件/结果记录，以及 Pause/Resume/Cancel。写路径包含 receipt 并发幂等闸门、显式层级锁序、`FOR UPDATE SKIP LOCKED`、Lease fencing、Run version/generation CAS，以及 Event、Checkpoint、Stage、Artifact 和后续动作的原子提交。

续租使用数据库时间校验并延长 Task/TaskAttempt 的同一 Lease，不推进 Run 版本；过期 Lease 回收使用数据库权威时间，原子结束旧 TaskAttempt、清除 Lease、追加 Event、推进 Run cursor，并转为 `retry_scheduled` 或 `dead_lettered`。失败事务会原子完成 attempt、清除 Lease、追加 Event，并区分 retry、不可重试终态与 Dead Letter。外部事件按 Event type 与 `match_key_hash` 单次消费 Wait，并实例化预存恢复计划。Tool 与 Agent 外部调用采用两阶段窗口：先提交 execution/Event 意图，再记录 adapter outcome；不确定结果持久化对账动作，backoff 必须持久化 `retry_at`。Agent Event Worker 会为 `running` 远端执行调用 `read_events`，从持久化 cursor 续读；每批事件在同一事务中完成确定性 receipt/raw digest 去重、本地 Event 追加、远端 cursor/version CAS、下一次轮询调度、Run 序列推进，以及规范化事件声明的 Task/Wait/Artifact/Execution outcome 投影。重复 Worker 会复用已提交的 Command Receipt，不会重复推进；远端批次报告终态时执行原子进入 `reconciling`，随后由 Status Worker 核验最终结果，终态后不再读取事件。Pause/Cancel 后的迟到结果保留审计，但业务投影受 Run version/generation/deadline fencing。若控制命令与提交响应并发，迟到的远端引用和协议版本会保留在 `stopping` 执行中；独立 stop Worker 使用稳定幂等身份调用注册 Adapter 的 `request_stop`，并以执行版本 CAS 记录为终态、`reconciling` 或 `manual_review`。`status_poll_at` 是远端下一次持久化轮询时间：`running` 时驱动事件读取，`reconciling` 时驱动状态查询；该时间不复用提交重试的 `retry_at`。Runtime 的有界 Scheduler tick 会为到期候选生成确定性的 Command/Event/Task/Receipt 身份，并隔离单候选失败。恢复 Worker 只领取 `reconcile` Task，领取结果携带 Task 输入和提交后的 Run version；Worker 校验恢复输入并提交 Tool retry attempt 或 Agent submission reconciliation 启动事务，启动事务会原子完成该一次性恢复 Task，事务成功后才调用外部 dispatcher。未知提交结果会先按原始幂等身份调用 `reconcile_submission`：发现远端执行时只补录引用，确认不存在后才调用幂等 `submit`，查询仍不确定时继续保留为 `outcome_unknown`，不会盲目重复创建。通用 dispatcher 已实现 tenant-scoped 请求装载、Adapter Registry、临时鉴权/trace/deadline 上下文解析、幂等重放能力闸门、统一错误分类、确定性结果命令以及 Store 回写。Scheduler、Recovery Worker、Agent Event/Stop/Status Worker 与 Lease Reclaimer 已通过通用 `PollingService` 接入有界并发、busy/idle/error 退避和优雅停机。

每个权威 Event 还会在同一 PostgreSQL 事务中创建唯一 `(tenant, event, topic)` 的 `run.events` Outbox 消息。Outbox Publisher 使用数据库时间领取短 Lease，发布成功后以 publisher/token/attempt fencing 确认；失败会持久化下一次可用时间，进程在外部发送后、确认前崩溃则在 Lease 到期后按 at-least-once 语义重放。旧 Publisher 无法确认被新 Worker 接管的消息。MVP 的真实 Publisher 输出结构化 JSON 日志；接入 Broker 时只需替换 Publisher，消费者仍须按 `event_id` 幂等。

Run 创建事务同时保存不可变 PlanRevision 1。后续完整 ExecutionPlan 快照可通过 `/v1/runs/{run_id}/plan-revisions` 幂等提交和查询；Store 同时 fence Run version、execution generation 与当前 Plan revision，并原子追加 `run.plan_revised` Event/Outbox，因此并发或过期 Replan 不会覆盖已提交计划。

## MVP 快速开始

MVP 会在启动时自动执行 PostgreSQL migration、创建一个由 `AGENT_LOOM_TENANT_KEY` 标识的开发 Tenant，并启动 HTTP API、交付 Worker、Scheduler、Recovery Worker、Agent Event/Stop/Status Worker、Outbox Publisher、Lease Reclaimer 和超时维护服务。详细边界与 API 示例见 [MVP 使用说明](./MVP.md)。

```bash
export AGENT_LOOM_DATABASE_URL='postgresql://agent_loom:agent_loom@127.0.0.1:5432/agent_loom'
export AGENT_LOOM_API_KEY='replace-with-at-least-16-characters'
cargo run -p agent-loom-server
```

创建一个交付 Run：

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H "authorization: Bearer $AGENT_LOOM_API_KEY" \
  -H 'content-type: application/json' \
  -H 'idempotency-key: delivery-001' \
  -d '{"input":{"goal":"交付 Agent Loom MVP"}}'
```

后台 Worker 会推进八个必需 Stage；fixture 会让第一次集成测试进入 `rework_required`，原子创建 implementation/self_test/integration_test attempt 2，审批通过后经正式 ToolExecution 执行部署，最后进入 `completed`。Run ID 可用于查询状态、Stage、Artifact、Pending Action 和 SSE Event。

默认继续使用确定性的本地 Mock。配置四个外部变量后，服务会切换到真实 HTTP Agent Server 和 DevOps Tool profile：

```bash
export AGENT_LOOM_AGENT_BASE_URL='https://agent.example.com'
export AGENT_LOOM_AGENT_TOKEN='short-lived-agent-token'
export AGENT_LOOM_DEVOPS_BASE_URL='https://deploy.example.com'
export AGENT_LOOM_DEVOPS_TOKEN='short-lived-devops-token'
```

四项必须同时设置；远程 HTTP 仅允许 loopback 开发地址，非 loopback Endpoint 必须使用 HTTPS。版本化端点、信封和安全约束见 [HTTP Adapter Profile](./HTTP_ADAPTER_PROFILE.md)。

真实联调不要直接填写供应商原生 API URL。目标服务需要实现 `agent-loom-http-v1`，或通过 gateway 完成协议映射。可以先对已有远程资源运行不产生副作用的探针：

```bash
export AGENT_LOOM_LIVE_AGENT_RUN_ID='remote-run-123'
export AGENT_LOOM_LIVE_DEPLOYMENT_REF='deployment-123'
cargo run -p agent-loom-adapter-http --bin live_probe
```

## 设计文档

- [产品需求](./REQUIREMENT.md)
- [技术计划](./PLAN.md)
- [状态机](./STATE_MACHINE.md)
- [领域模型](./DOMAIN_MODEL.md)
- [存储契约](./STORE_CONTRACT.md)
- [Adapter 契约](./ADAPTER_CONTRACT.md)
- [HTTP Adapter Profile](./HTTP_ADAPTER_PROFILE.md)
- [业产研 E2E 场景](./E2E_SCENARIO.md)
- [PostgreSQL/MySQL 迁移设计](./MIGRATION_DESIGN.md)

## 本地验证

```bash
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

设置测试专用数据库后可同时执行真实 migration 与事务 smoke test；后者还覆盖查询分页、续租与失败幂等、Lease fencing、Wait 单次消费与恢复 Task、暂停/恢复/取消幂等、取消与 Agent 提交响应竞态、远端 stop 调度，以及 `cancel`/`complete_task` 并发终态唯一性：

```bash
AGENT_LOOM_TEST_POSTGRES_URL='postgresql://...' cargo test \
  -p agent-loom-server \
  -p agent-loom-store-postgres \
  -p agent-loom-provider-conformance \
  -- --test-threads=1
```

Phase 2B 的 MySQL 8.4 迁移、连接池与会话策略可使用独立测试库验证：

```bash
AGENT_LOOM_TEST_MYSQL_URL='mysql://agent_loom:password@127.0.0.1:3306/agent_loom_test' \
  cargo test -p agent-loom-store-mysql -p agent-loom-provider-conformance \
  --all-targets -- --test-threads=1
```

数据库测试会执行 migration，并可能创建测试 Tenant/Run；不要指向生产数据库。

GitHub Actions 会分别执行 workspace 质量门禁、PostgreSQL 16 真实事务测试和 MySQL 8.4 migration/session 测试。MySQL 的完整 `DurableStore` 黑盒场景会随 Phase 2B 事务命令落地逐项接入。
