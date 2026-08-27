# Agent Loom HTTP Adapter Profile v1

`agent-loom-http-v1` 是 Phase 2A 的首个真实网络 Adapter profile。它把远程 Agent Server 和 DevOps 部署服务映射到 `adapter-core` 的稳定领域契约，不允许 Adapter 直接写 DurableStore。

## 安全和调用边界

- 非 loopback Endpoint 必须使用 HTTPS；`http://` 只允许本地受控测试。
- Base URL 不允许携带用户名、密码、query 或 fragment。
- HTTP redirect 被禁用，避免跨 Host 转发 Authorization。
- 每次调用使用绝对 deadline，并在读取完整响应前限制响应字节数。
- Bearer token 只由调用上下文短暂解析，`Debug`、错误和协议 payload 均不包含 token。
- 所有副作用请求携带稳定 `Idempotency-Key`、correlation ID、execution ID、request digest 和 W3C trace headers。
- submit/deploy/rollback 发生 timeout 或传输结果不确定时返回 `SubmissionUncertain`/`Uncertain`，不盲目重放副作用。

## Agent Server API

所有响应的 `protocol_version` 必须为 `agent-loom-http-v1`。

| 操作 | HTTP |
| --- | --- |
| 提交 | `POST /v1/agent-runs` |
| 按幂等键对账 | `GET /v1/agent-runs/by-idempotency?key=...` |
| 查询状态/结果 | `GET /v1/agent-runs/{run_id}` |
| 有界读取 Event | `GET /v1/agent-runs/{run_id}/events` |
| 协作停止 | `POST /v1/agent-runs/{run_id}/stop` |

提交请求：

```json
{
  "instructions": "produce the requested artifact",
  "input": {},
  "budget": {
    "max_duration_micros": 30000000,
    "max_output_bytes": 1048576
  }
}
```

提交或对账响应：

```json
{
  "run_id": "remote-run-123",
  "session_id": "optional-session",
  "protocol_version": "agent-loom-http-v1"
}
```

Event 列表响应必须包含 `events`、`next_cursor` 和 `terminal`。每个 Event 包含可选稳定 `id`/`sequence`、非空 `kind`、`authoritative` 和 JSON `payload`。相同 cursor 的读取必须可安全重放。

## DevOps API

| 操作 | HTTP |
| --- | --- |
| 创建部署 | `POST /v1/deployments` |
| 查询部署与健康状态 | `GET /v1/deployments/{external_ref}` |
| 回滚 | `POST /v1/deployments/{external_ref}/rollback` |

部署执行返回以下三种信封之一：

```json
{"status":"completed","result":{}}
{"status":"accepted","external_ref":"deployment-123"}
{"status":"uncertain","external_ref":null}
```

异步查询只有在 `status=completed`、`healthy=true` 且存在 `result` 时才映射为成功；HTTP 2xx 本身不代表部署成功。Rollback 同样返回 `completed`、`accepted` 或 `uncertain`。

## Conformance

`adapter-core::conformance` 提供可复用黑盒 runner。`adapter-http` 使用受控 HTTP Fake Server 验证：

- 同一 Agent/部署幂等键返回同一远程引用；
- Agent accepted、completed 和 stop/complete 竞争不会混淆；
- submit 可按幂等键对账，Event cursor 可重放；
- 部署必须经过健康查询，rollback 必须被确认；
- 认证失败分类稳定且不会泄漏凭据；
- Endpoint TLS 和 URL 策略在创建 Adapter 时强制执行。

## 使用真实生产 Endpoint 联调

生产服务必须原生实现上面的 `agent-loom-http-v1` 路由，或者通过 gateway 把它映射到供应商 API。不能把 OpenAI、GitHub Actions、Argo CD 等原生 Base URL 直接配置为 Agent Loom Endpoint；这些 API 的路由、信封、幂等和状态语义都不同。

建议按以下顺序联调：

1. 在预生产环境部署 Agent gateway 和 DevOps gateway，只给测试租户/测试项目最小权限凭据。
2. 先用已有远程 Run 和 Deployment 做只读探测；探针不会提交 Agent Run、创建部署或触发回滚。
3. 只读探测通过后，在隔离的 canary 项目执行一条有副作用的完整 Run，使用唯一幂等键并保留双方审计日志。
4. 对照 Agent Loom Event、远程 Run/Deployment 状态和 correlation ID，验证超时后的 reconcile，不以一次 HTTP 2xx 代替最终成功判断。

只读探测命令：

```bash
export AGENT_LOOM_AGENT_BASE_URL='https://agent-gateway.staging.example.com'
export AGENT_LOOM_AGENT_TOKEN='short-lived-read-token'
export AGENT_LOOM_DEVOPS_BASE_URL='https://deploy-gateway.staging.example.com'
export AGENT_LOOM_DEVOPS_TOKEN='short-lived-read-token'

# 二选一或同时设置；已有资源 ID 来自目标系统。
export AGENT_LOOM_LIVE_AGENT_RUN_ID='remote-run-123'
export AGENT_LOOM_LIVE_DEPLOYMENT_REF='deployment-123'
export AGENT_LOOM_LIVE_IDEMPOTENCY_KEY='live-probe-20260827'

cargo run -p agent-loom-adapter-http --bin live_probe
```

如果没有 `AGENT_LOOM_LIVE_AGENT_RUN_ID`，探针会调用幂等键对账接口查找已有 Run；如果没有 `AGENT_LOOM_LIVE_DEPLOYMENT_REF`，探针只确认 DevOps 配置已装载，不会制造一个部署来探活。凭据应由 Secret Manager/JIT 系统注入，避免写入仓库或 shell history。

完整 canary 联调时，用同样四个 Base URL/Token 环境变量启动 `agent-loom-server`，再通过 `/v1/runs` 创建测试 Run。服务启动配置要求四项同时存在，并会拒绝非 loopback 的明文 HTTP URL。
