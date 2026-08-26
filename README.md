# Agent Loom

Agent Loom 是面向复杂 Agent 团队协作的持久化流程运行时。首个场景覆盖需求分析、原型/PRD、技术方案、编码、自测、集成测试和 DevOps 部署。

## 当前工程边界

```text
crates/domain          共享 ID、值对象、状态与只读投影
crates/durable-store   DurableStore 接口、命令、结果、错误和 conformance 清单
crates/adapter-core    Agent Server / Tool Adapter 通用接口
crates/runtime         Scheduler/Worker/Adapter 的数据库无关编排与服务生命周期
crates/server          可运行的 PostgreSQL MVP、HTTP API 与 Mock 业产研 Worker
crates/provider-conformance  PostgreSQL/MySQL 共享迁移与行为契约测试
crates/store-postgres  PostgreSQL Provider 与物理迁移
crates/store-mysql     MySQL/InnoDB Provider 与物理迁移
```

Runtime 内部按职责组织：

```text
runtime/src/adapter/    Registry、调用上下文、恢复分发与结果回写
runtime/src/recovery/   reconcile Task 领取与外部执行启动事务
runtime/src/scheduler/  due-work 扫描、确定性计划与原子应用
runtime/src/service/    有界轮询、退避、关闭信号与 Job 接线
```

`domain`、`durable-store` 与 `adapter-core` 继续保持零外部依赖；Provider crate 可以引入各自的数据库驱动和异步运行时，但驱动类型不得泄漏到共享领域契约。`server` 是 PostgreSQL 专用的产品装配层，不改变 Runtime 与 Store 共享契约的数据库无关性。

已实现 `0000_migration_meta` 至 `0010_agent_invocation_envelope` 的 PostgreSQL/MySQL 对等迁移，覆盖定义身份、Agent Endpoint、Run、Event、CommandReceipt、Stage、Task、TaskAttempt、Checkpoint、Wait、Artifact、ToolExecution 与 AgentExecution。`provider-conformance` 会校验逻辑迁移顺序、表归属、终态 Event 与 Checkpoint 归属约束、Task Lease、Wait 单次消费槽与恢复计划、Tool/Agent 重试时间、Agent 请求信封、Artifact 版本血缘、外部执行幂等与 Agent Event 去重；同时提供只依赖 `dyn DurableStore` 的黑盒行为场景，首个场景覆盖 Lease 续租与回收幂等、过期重试投影、attempt 递增与再次领取。

PostgreSQL 已接入真实驱动执行层：migration executor 使用 SHA-256 physical checksum、session advisory lock、step journal 和逐批 schema introspection；`PostgresStore` 通过连接池完整实现对象安全的 `DurableStore`，事务垂直切片已覆盖 Run 创建/查询、Event 分页、Task 生命周期、Wait 事件应用、ToolExecution 准备/结果记录、AgentExecution 提交/事件/结果记录，以及 Pause/Resume/Cancel。写路径包含 receipt 并发幂等闸门、显式层级锁序、`FOR UPDATE SKIP LOCKED`、Lease fencing、Run version/generation CAS，以及 Event、Checkpoint、Stage、Artifact 和后续动作的原子提交。

续租使用数据库时间校验并延长 Task/TaskAttempt 的同一 Lease，不推进 Run 版本；过期 Lease 回收使用数据库权威时间，原子结束旧 TaskAttempt、清除 Lease、追加 Event、推进 Run cursor，并转为 `retry_scheduled` 或 `dead_lettered`。失败事务会原子完成 attempt、清除 Lease、追加 Event，并区分 retry、不可重试终态与 Dead Letter。外部事件按 Event type 与 `match_key_hash` 单次消费 Wait，并实例化预存恢复计划。Tool 与 Agent 外部调用采用两阶段窗口：先提交 execution/Event 意图，再记录 adapter outcome；不确定结果持久化对账动作，backoff 必须持久化 `retry_at`。Agent 事件批次会原子完成 receipt/raw digest 去重、本地 Event 追加、远端 cursor CAS、Run 序列推进，以及规范化事件声明的 Task/Wait/Artifact/Execution outcome 投影；Pause/Cancel 后的迟到结果保留审计，但业务投影受 Run version/generation/deadline fencing。Runtime 的有界 Scheduler tick 会为到期候选生成确定性的 Command/Event/Task/Receipt 身份，并隔离单候选失败。恢复 Worker 只领取 `reconcile` Task，领取结果携带 Task 输入和提交后的 Run version；Worker 校验恢复输入并提交 Tool retry attempt 或 Agent resubmit 启动事务，启动事务会原子完成该一次性恢复 Task，事务成功后才调用外部 dispatcher。通用 dispatcher 已实现 tenant-scoped 请求装载、Adapter Registry、临时鉴权/trace/deadline 上下文解析、幂等重放能力闸门、统一错误分类、确定性结果命令以及 Store 回写。Scheduler、Recovery Worker 与 Lease Reclaimer 已通过通用 `PollingService` 接入有界并发、busy/idle/error 退避和优雅停机。

## MVP 快速开始

MVP 会在启动时自动执行 PostgreSQL migration、创建一个由 `AGENT_LOOM_TENANT_KEY` 标识的开发 Tenant，并启动 HTTP API、交付 Worker、Scheduler、Recovery Worker、Lease Reclaimer 和超时维护服务。详细边界与 API 示例见 [MVP 使用说明](./MVP.md)。

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

## 设计文档

- [产品需求](./REQUIREMENT.md)
- [技术计划](./PLAN.md)
- [状态机](./STATE_MACHINE.md)
- [领域模型](./DOMAIN_MODEL.md)
- [存储契约](./STORE_CONTRACT.md)
- [Adapter 契约](./ADAPTER_CONTRACT.md)
- [业产研 E2E 场景](./E2E_SCENARIO.md)
- [PostgreSQL/MySQL 迁移设计](./MIGRATION_DESIGN.md)

## 本地验证

```bash
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

设置测试专用数据库后可同时执行真实 migration 与事务 smoke test；后者还覆盖查询分页、续租与失败幂等、Lease fencing、Wait 单次消费与恢复 Task、暂停/恢复/取消幂等，以及 `cancel`/`complete_task` 并发终态唯一性：

```bash
AGENT_LOOM_TEST_POSTGRES_URL='postgresql://...' cargo test \
  -p agent-loom-server \
  -p agent-loom-store-postgres \
  -p agent-loom-provider-conformance \
  -- --test-threads=1
```

数据库测试会在目标库创建唯一测试 Tenant/Run 作为审计记录；不要指向生产数据库。

GitHub Actions 会分别执行 workspace 质量门禁和 PostgreSQL 16 真实事务测试。数据库 Job 通过 `AGENT_LOOM_TEST_POSTGRES_URL` 启用 migration、事务垂直切片和 Provider 黑盒场景，并使用单线程测试避免同库迁移用例并发干扰。
