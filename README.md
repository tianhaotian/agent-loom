# Agent Loom

Agent Loom 是面向复杂 Agent 团队协作的持久化流程运行时。首个场景覆盖需求分析、原型/PRD、技术方案、编码、自测、集成测试和 DevOps 部署。

## 当前工程边界

```text
crates/domain          共享 ID、值对象、状态与只读投影
crates/durable-store   DurableStore 接口、命令、结果、错误和 conformance 清单
crates/adapter-core    Agent Server / Tool Adapter 通用接口
crates/provider-conformance  PostgreSQL/MySQL 共享迁移与行为契约测试
crates/store-postgres  PostgreSQL Provider 与物理迁移
crates/store-mysql     MySQL/InnoDB Provider 与物理迁移
```

`domain`、`durable-store` 与 `adapter-core` 继续保持零外部依赖；Provider crate 可以引入各自的数据库驱动和异步运行时，但驱动类型不得泄漏到共享领域契约。

已实现 `0000_migration_meta` 至 `0006_external_executions` 的 PostgreSQL/MySQL 对等迁移，覆盖定义身份、Agent Endpoint、Run、Event、CommandReceipt、Stage、Task、TaskAttempt、Checkpoint、Wait、Artifact、ToolExecution 与 AgentExecution。`provider-conformance` 会校验逻辑迁移顺序、表归属、终态 Event 与 Checkpoint 归属约束、Task Lease、Wait 单次消费槽、Artifact 版本血缘、外部执行幂等与 Agent Event 去重。

PostgreSQL 已接入真实驱动执行层：migration executor 使用 SHA-256 physical checksum、session advisory lock、step journal 和逐批 schema introspection；事务垂直切片已覆盖 `create_run`、`get_run`、`list_events`、`claim_task`、`complete_task`、`pause_run`、`resume_run` 与 `cancel_run`。写路径包含 receipt 并发幂等闸门、显式 `Run → Task` 锁序、`FOR UPDATE SKIP LOCKED`、Lease fencing、Run version/generation CAS，以及 Event、Checkpoint、Stage、Artifact 和后续动作的原子提交。

Pause 会递增 execution generation、终止旧 Lease attempt、保留并冻结动态 Task 计划；Resume 会拒绝存在未知外部结果的 Run，并根据当前 Task/Wait 重新投影状态；Cancel 会原子关闭 Task、Wait、Stage，设置唯一 terminal Event，并将远程 Agent stop 与不确定 Tool 对账意图持久化。该切片目前接受连接池借出的 `&mut tokio_postgres::Client`；完整 `DurableStore` Provider 仍需补齐连接池封装、续租、失败、事件应用和外部执行操作。

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

设置测试专用数据库后可同时执行真实 migration 与事务 smoke test；后者还覆盖查询分页、暂停/恢复/取消幂等，以及 `cancel`/`complete_task` 并发终态唯一性：

```bash
AGENT_LOOM_TEST_POSTGRES_URL='postgresql://...' cargo test -p agent-loom-store-postgres
```

smoke test 会在目标数据库创建唯一测试 Tenant/Run 作为审计记录；不要指向生产数据库。
