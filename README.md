# Agent Loom

Agent Loom 是面向复杂 Agent 团队协作的持久化流程运行时。首个场景覆盖需求分析、原型/PRD、技术方案、编码、自测、集成测试和 DevOps 部署。

## 当前工程边界

```text
crates/domain          共享 ID、值对象、状态与只读投影
crates/durable-store   DurableStore 接口、命令、结果、错误和 conformance 清单
crates/adapter-core    Agent Server / Tool Adapter 通用接口
crates/store-postgres  PostgreSQL Provider 与物理迁移
crates/store-mysql     MySQL/InnoDB Provider 与物理迁移
```

当前阶段先保持核心 crate 零外部依赖，冻结领域和事务边界；数据库驱动、异步运行时和 HTTP 实现将在 Provider conformance harness 建立后接入。

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

