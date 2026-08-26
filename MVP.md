# Agent Loom MVP

## 交付范围

当前 MVP 是一个 PostgreSQL 单 Provider、单开发 Tenant 的可运行垂直切片，目标是验证 Agent Loom 的持久化执行语义能够组成完整服务，而不是提供生产级多租户控制面。

已包含：

- 启动时自动执行 `0000` 至 `0010` migration；
- PostgreSQL 连接池与对象安全的 `DurableStore`；
- API Key 认证边界和 tenant-scoped 查询/命令；
- Workflow、Run、Stage、Artifact、Pending Action 和 Event HTTP 查询；
- 可从 `Last-Event-ID` 或 Event sequence 断点续读的 SSE；
- 八个必需 Stage，以及集成测试失败后原子创建的三阶段 attempt 2 返工链；
- 部署审批 Wait、重复事件幂等消费和持久化恢复 Task；
- 通过正式 Adapter/Execution 契约执行的 Mock Agent Server 与 Mock DevOps Tool；
- Scheduler、Recovery Worker、过期 Lease 回收和 deadline/Wait/stale execution 维护服务；
- 幂等命令、Run version 和 execution generation fencing；
- JSON 结构化请求/Worker 日志和响应关联 ID；
- 从 HTTP 创建到 PostgreSQL 终态、返工、审批、Tool 部署和 deadline 的自动化 E2E 验收。

MVP 暂不包含 MySQL 事务 Provider、生产 SSO/RBAC、真实外部 Agent/DevOps 服务、子 Run/Fan-out/Fan-in 和生产可观测平台。这些属于 Phase 2B/3 或生产化扩展，不影响当前 PostgreSQL MVP 的权威执行闭环。

## 配置

| 环境变量 | 必需 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `AGENT_LOOM_DATABASE_URL` | 是 | 无 | PostgreSQL 连接 URL |
| `AGENT_LOOM_BIND` | 否 | `127.0.0.1:8080` | HTTP 监听地址 |
| `AGENT_LOOM_TENANT_KEY` | 否 | `mvp-local` | 单 Tenant 稳定身份 |
| `AGENT_LOOM_API_KEY` | 是 | 无 | 16 至 255 字符的 HTTP Bearer/API Key |
| `AGENT_LOOM_POOL_SIZE` | 否 | `8` | PostgreSQL 连接池上限 |

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

### 查询 Run 与 Event

```bash
curl -sS -H 'authorization: Bearer replace-with-at-least-16-characters' \
  http://127.0.0.1:8080/v1/runs/RUN_ID
curl -sS -H 'authorization: Bearer replace-with-at-least-16-characters' \
  'http://127.0.0.1:8080/v1/runs/RUN_ID/events?after=0&limit=100'
```

所有 `/v1` 请求都必须携带 `Authorization: Bearer ...` 或 `X-API-Key`。还可查询 `/stages`、`/artifacts`、`/pending-actions` 和 `/events/stream`；SSE 断线后使用 `Last-Event-ID` 恢复。

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
