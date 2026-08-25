# Agent Loom Agent Server 与 Tool Adapter 契约

## 1. 目的与资料基线

本文定义 Agent Loom 如何以远程 Server/Gateway/API 模式接入第三方 Agent 和外部工具。Adapter 负责协议转换与外部 I/O，不拥有 Run 状态，不直接写 DurableStore，也不通过 `spawn` CLI 子进程托管第三方 Agent。

当前能力映射以 2026-08-26 可访问的官方资料为基线：

- [OpenClaw Gateway Protocol](https://docs.openclaw.ai/gateway/protocol)：WebSocket RPC、协议协商、能力发现、事件和 session abort；
- [Hermes Programmatic Integration](https://hermes-agent.nousresearch.com/docs/developer-guide/programmatic-integration)：Run 创建、状态、SSE、审批、steer、stop 与 capabilities；
- [OpenAI Responses create](https://developers.openai.com/api/reference/cli/resources/responses/methods/create)、[retrieve](https://developers.openai.com/api/reference/cli/resources/responses/methods/retrieve) 和 [cancel](https://developers.openai.com/api/reference/cli/resources/beta/subresources/responses)：后台执行、状态/事件续读和取消。

外部协议会演进。本文只把规范化领域语义视为稳定契约；具体端点、字段和协议版本必须由 Adapter profile 固定并通过能力探测验证。

## 2. Adapter 边界

### 2.1 AgentServerAdapter

AgentServerAdapter 连接一个能够独立运行多步 Agent Loop 的远程服务。远程执行可以包含模型推理、工具调用、审批、子 Agent、产物和中途指导。

典型对象：OpenClaw Gateway、Hermes API Server、OpenAI Responses/Codex-compatible server。

### 2.2 ToolAdapter

ToolAdapter 调用一个边界清晰的外部能力。它不拥有通用 Agent Loop，例如：

- Git 仓库、代码评审和合并；
- 原型、PRD 或文档系统；
- 测试平台、质量扫描和制品库；
- CI/CD、环境部署、回滚和发布确认；
- MCP、HTTP API、数据检索和文件处理。

### 2.3 禁止越界

- Adapter 不得直接更新 Run、StageExecution、Task、Checkpoint 或 Event。
- Adapter 不得自行生成新的 Runtime 幂等键。
- Adapter 不得把远程“已接受”映射为本地“已完成”。
- Adapter 不得在错误信息中泄漏凭据、完整请求体或第三方内部堆栈。
- Adapter 不得将 CLI 文本输出、终端 ANSI 流或未版本化私有结构声明为稳定生产协议。
- Runtime Worker 不得为了等待远程 Run 完成而长期占用 Lease；提交后必须创建 WaitSubscription、Webhook 等待或有限 poll Task。

## 3. Crate 与注册模型

```text
crates/
├── adapter-core/             # 公共类型、trait、错误、conformance suite
├── adapter-openclaw/         # OpenClaw Gateway profile
├── adapter-hermes/           # Hermes HTTP/SSE profile
├── adapter-openai-responses/ # OpenAI Responses profile
├── adapter-codex-server/     # Codex Server profile；协议未冻结前为 experimental
├── tool-git/
├── tool-test-platform/
└── tool-devops/
```

Adapter Registry 以 `(adapter_kind, contract_version)` 定位实现。AgentEndpoint 保存 endpoint、protocol range、credential_ref 和允许的 Adapter profile；AgentExecution 保存提交时的能力快照与实际协议版本。

## 4. 公共调用上下文

```rust
use std::{future::Future, pin::Pin};

pub type AdapterFuture<'a, T> =
    Pin<Box<dyn Future<Output = AdapterResult<T>> + Send + 'a>>;

pub type AdapterResult<T> = Result<T, AdapterError>;
```

```rust
pub struct AdapterCallContext {
    pub tenant_id: TenantId,
    pub execution_id: ExecutionId,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<CausationId>,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
    pub deadline: Instant,
    pub trace_context: TraceContext,
    pub auth: ResolvedAuth,
}
```

- `ResolvedAuth` 由 CredentialResolver 在调用前短暂解析，不可序列化到 Event、Checkpoint 或日志。
- Adapter 必须使用绝对 deadline，不能各层叠加无界 timeout。
- 所有重试复用同一 execution ID、idempotency key 和 request hash。
- Adapter 必须把 trace context 映射到第三方支持的标准追踪头；不支持时仍记录本地 span。

## 5. 能力模型

### 5.1 AgentCapabilities

```rust
pub struct AgentCapabilities {
    pub contract_version: AdapterContractVersion,
    pub protocol: ProtocolDescriptor,
    pub submission: SubmissionCapabilities,
    pub status: StatusCapabilities,
    pub events: EventCapabilities,
    pub stop: StopCapabilities,
    pub approvals: ApprovalCapabilities,
    pub guidance: GuidanceCapabilities,
    pub sessions: SessionCapabilities,
    pub artifacts: ArtifactCapabilities,
    pub limits: AdapterLimits,
}
```

```rust
pub struct SubmissionCapabilities {
    pub asynchronous: bool,
    pub idempotency: SubmissionIdempotency,
    pub returns_remote_run_ref: bool,
}

pub enum SubmissionIdempotency {
    GuaranteedByRemote,
    QueryByIdempotencyKey,
    QueryByClientExecutionId,
    RemoteReferenceOnly,
    Unsupported,
}

pub struct EventCapabilities {
    pub transport: EventTransport,
    pub resumable: bool,
    pub ordered: bool,
    pub source_event_ids: bool,
    pub cursor_kind: CursorKind,
}

pub enum EventTransport {
    None,
    Poll,
    Sse,
    WebSocket,
    Webhook,
}

pub enum StopSemantics {
    Unsupported,
    Cooperative,
    ConfirmedTerminal,
}
```

能力必须来自握手、capabilities endpoint、固定协议 profile 或官方 API schema，不能根据产品名称猜测。

### 5.2 必需能力匹配

AgentDefinition 或 Workflow Stage 可以声明：

```rust
pub struct RequiredAgentCapabilities {
    pub async_run: bool,
    pub resumable_events: bool,
    pub stop: MinimumStopSemantics,
    pub approvals: bool,
    pub guidance: bool,
    pub artifacts: Vec<ArtifactKind>,
}
```

- Runtime 必须在创建 AgentExecution 前完成 capability match。
- 缺少强制能力返回 `ADAPTER_CAPABILITY_MISSING`，不得先提交再猜测降级行为。
- 可选能力可以降级，例如 SSE 降级为有限轮询；降级结果写入 AgentExecution capability snapshot。
- Pause 需要停止能力但远程只支持 cooperative 时，Run 可以进入 paused，但 Resume 必须等待停止确认或对账结果。

### 5.3 能力快照

- probe 结果包含服务版本、协议版本、能力和限制。
- 每次 AgentExecution 保存不可变 snapshot；Endpoint 后续升级不改变历史执行解释。
- 重连后发现协议版本变化时，Adapter 必须重新验证兼容范围。
- 运行中能力降低不能静默忽略，应产生 `adapter.capability_changed` 审计信息并决定继续、降级或停止。

## 6. 规范化 Agent 模型

### 6.1 请求

```rust
pub struct AgentRunRequest {
    pub execution_id: AgentExecutionId,
    pub agent: AgentSelector,
    pub instructions: String,
    pub input: Vec<AgentInputItem>,
    pub conversation: Option<ConversationRef>,
    pub workspace: Option<WorkspaceRef>,
    pub artifacts: Vec<ArtifactInputRef>,
    pub tool_policy: ToolPolicy,
    pub approval_policy: ApprovalPolicy,
    pub budget: ExecutionBudget,
    pub metadata: SafeMetadata,
}
```

`WorkspaceRef` 表示远程服务可访问的仓库、工作区或对象存储引用，不是要求 Runtime 在本机创建目录。生产 Adapter 不接受“运行某个本地 CLI 路径”作为 workspace。

### 6.2 远程引用

```rust
pub struct RemoteAgentRef {
    pub remote_run_id: String,
    pub remote_session_id: Option<String>,
    pub protocol_version: String,
}
```

- remote ID 只在 `(tenant, endpoint)` 作用域内解释。
- Adapter 返回 remote ref 后，Runtime 必须通过 `record_agent_submission` 持久化。
- 缺少 remote ref 不代表远程未接受，必须根据 submission idempotency 能力对账。

### 6.3 规范化状态

```rust
pub enum NormalizedAgentStatus {
    Accepted,
    Running,
    WaitingForApproval,
    WaitingForInput,
    Stopping,
    Completed,
    Failed,
    Cancelled,
    Unknown,
}
```

状态映射必须保留原始 vendor status 作为受控诊断字段。`Accepted` 只表示远程接收请求；只有 `Completed` 且结果契约验证成功后才能推进本地 Task。

## 7. AgentServerAdapter Trait

```rust
pub trait AgentServerAdapter: Send + Sync {
    fn kind(&self) -> AdapterKind;
    fn contract_version(&self) -> AdapterContractVersion;

    fn probe<'a>(
        &'a self,
        endpoint: &'a AgentEndpointConfig,
        auth: &'a ResolvedAuth,
    ) -> AdapterFuture<'a, ProbeResult>;

    fn submit<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        endpoint: &'a AgentEndpointConfig,
        request: AgentRunRequest,
    ) -> AdapterFuture<'a, SubmitAgentOutcome>;

    fn reconcile_submission<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        endpoint: &'a AgentEndpointConfig,
        probe: SubmissionProbe,
    ) -> AdapterFuture<'a, SubmissionReconcileOutcome>;

    fn get_status<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        endpoint: &'a AgentEndpointConfig,
        remote: &'a RemoteAgentRef,
    ) -> AdapterFuture<'a, RemoteAgentSnapshot>;

    fn read_events<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        endpoint: &'a AgentEndpointConfig,
        remote: &'a RemoteAgentRef,
        cursor: Option<OpaqueRemoteCursor>,
        limits: EventReadLimits,
    ) -> AdapterFuture<'a, RemoteEventBatch>;

    fn request_stop<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        endpoint: &'a AgentEndpointConfig,
        remote: &'a RemoteAgentRef,
        reason: StopReason,
    ) -> AdapterFuture<'a, StopRequestOutcome>;

    fn resolve_approval<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        endpoint: &'a AgentEndpointConfig,
        remote: &'a RemoteAgentRef,
        decision: ApprovalDecision,
    ) -> AdapterFuture<'a, ApprovalOutcome>;

    fn send_guidance<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        endpoint: &'a AgentEndpointConfig,
        remote: &'a RemoteAgentRef,
        guidance: AgentGuidance,
    ) -> AdapterFuture<'a, GuidanceOutcome>;
}
```

不支持的方法返回 `ADAPTER_CAPABILITY_MISSING`，不能返回伪成功。

## 8. Agent 提交与恢复

### 8.1 标准提交序列

```text
DurableStore.prepare_agent_execution
  → commit submitting intent
  → AgentServerAdapter.submit
  → DurableStore.record_agent_submission
  → commit remote ref + WaitSubscription/poll Task + event cursor
  → release Worker
```

- Adapter.submit 不得内部无限重试。
- 请求超时但远程可能已接受时返回 `SubmissionUncertain`，不是普通 transient error。
- Runtime 将不确定提交记为 AgentExecution `outcome_unknown` 并创建 reconcile Task。

### 8.2 Reconcile

按能力优先级：

1. 按远程幂等键查询；
2. 按 client execution ID 查询；
3. 使用已知 remote ref 查询；
4. 查询安全审计列表并做唯一关联；
5. 无法确认时进入 manual_review。

只有能力快照明确允许幂等重放时，reconcile 才能重新 submit。重新提交仍复用原 key 和 execution ID。

### 8.3 Poll 与流式事件

- `read_events` 必须是有限操作，由 `max_events`、`max_bytes` 和 `max_wait` 共同约束。
- Worker 不持有无限 SSE/WebSocket；可使用短时读取 Task，或独立无状态 Connector 以持久化 cursor 分批提交。
- 连接断开后从 AgentExecution cursor 续读；远程不支持续读时，Adapter 使用 status/result 对账，不伪造遗漏事件。
- Token delta 可以实时广播但不是状态权威；审批、工具、产物和终态事件必须持久化。

## 9. 远程事件规范化

### 9.1 Event envelope

```rust
pub struct NormalizedAgentEvent {
    pub source_event_id: Option<String>,
    pub source_sequence: Option<u64>,
    pub kind: AgentEventKind,
    pub occurred_at: Option<Instant>,
    pub durability: EventDurability,
    pub payload: RedactedJson,
    pub raw_digest: Digest,
}

pub enum EventDurability {
    Authoritative,
    Transient,
}
```

### 9.2 事件类型

```text
run.accepted
run.started
message.delta                 # transient by default
message.completed
tool.started
tool.progress                 # transient or sampled
tool.completed
approval.required
input.required
artifact.produced
usage.updated
run.stopping
run.completed
run.failed
run.cancelled
heartbeat                     # transient
```

- `message.delta`、heartbeat 和高频 progress 不得直接推进 Run 状态。
- approval/input/tool/artifact/terminal 事件必须具有稳定 source ID，或由 Adapter 生成确定性 dedupe key。
- 没有 source ID 时，dedupe key 至少包含 endpoint、remote run、event kind、source sequence/cursor 和 canonical payload digest。
- 原始 payload 可以加密存为诊断附件；状态机只消费经过 schema 验证和敏感字段过滤的规范化 payload。

### 9.3 批次提交

Runtime 把 RemoteEventBatch 交给 `DurableStore.append_agent_events`。本地 Event、AgentExecution cursor、Wait/Task/Artifact 和状态投影必须同事务提交。批次失败时 cursor 不前进。

## 10. Stop、Pause、Approval 与 Guidance

### 10.1 Stop

```rust
pub enum StopRequestOutcome {
    Accepted { cooperative: bool },
    AlreadyTerminal { status: NormalizedAgentStatus },
    Unsupported,
    Uncertain,
}
```

- `Accepted` 表示停止请求已接收，不表示远程已经 cancelled。
- cooperative stop 需要后续 status/event 确认。
- Stop 调用自身必须使用稳定幂等键或稳定 stop logical key。
- Stop 与远程完成竞争时记录真实远程结果；本地 Run generation 决定迟到结果能否推进。
- Stop 不确定时 AgentExecution 进入 `outcome_unknown` 或保持 stopping 并创建 reconcile。

### 10.2 Approval

- 远程 approval.required 必须映射为本地 WaitSubscription，包含稳定 remote request ID。
- 人工决定先由 DurableStore 原子消费本地 Wait，再创建 `resolve_approval` Task。
- Adapter 调用成功后记录远程 accepted；远程继续运行由后续事件确认。
- 同一 approval decision 重复提交返回原结果；approve 与 reject 不能共用一个幂等键。

### 10.3 Guidance/Steer

- Guidance 是排队意图，不等于远程 Agent 已读取。
- Adapter 必须返回 `queued/consumed/rejected/uncertain` 中的明确结果。
- 只支持 follow-up、不支持 mid-run steer 的服务可以把 guidance 转为后续用户输入，但必须在 capability snapshot 中标明语义降级。
- 已终态远程 Run 不得接受 mid-run guidance；调用方应创建新的 Task/Run。

## 11. ToolAdapter Trait

### 11.1 Tool 描述

```rust
pub struct ToolDescriptor {
    pub tool_key: ToolKey,
    pub contract_version: AdapterContractVersion,
    pub input_schema: JsonSchema,
    pub output_schema: JsonSchema,
    pub side_effect: SideEffectClass,
    pub idempotency: ToolIdempotency,
    pub query_outcome: bool,
    pub compensation: CompensationCapability,
    pub limits: ToolLimits,
}

pub enum SideEffectClass {
    ReadOnly,
    IdempotentWrite,
    NonIdempotentWrite,
    CompensatableWrite,
}
```

### 11.2 Trait

```rust
pub trait ToolAdapter: Send + Sync {
    fn descriptor(&self) -> &ToolDescriptor;

    fn execute<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: ToolRequest,
    ) -> AdapterFuture<'a, ToolCallOutcome>;

    fn query_outcome<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        external: ExternalOperationRef,
    ) -> AdapterFuture<'a, ToolQueryOutcome>;

    fn compensate<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: CompensationRequest,
    ) -> AdapterFuture<'a, CompensationOutcome>;
}
```

不支持 query 或 compensation 时返回 capability missing。Runtime 必须在执行前根据 side-effect class 决定重试与审批策略。

### 11.3 Tool outcome

```rust
pub enum ToolCallOutcome {
    Completed(ToolResult),
    Accepted {
        external_ref: ExternalOperationRef,
        suggested_poll_after: Option<Duration>,
    },
    Rejected(AdapterError),
    Uncertain(UncertainOperation),
}
```

Accepted 的长任务必须创建 WaitSubscription 或 poll Task，并释放 Worker。Uncertain 不得自动转换为普通失败重试。

## 12. DevOps Adapter 约束

部署属于高风险副作用，ToolDescriptor 至少声明：

- 目标环境与环境保护级别；
- 部署制品 digest 和版本；
- 是否支持 dry-run、状态查询、取消、回滚；
- 幂等作用域，例如 environment + release ID；
- 所需审批策略；
- 终态成功条件和健康验证窗口。

标准部署流程：

```text
approval consumed
  → prepare ToolExecution
  → deploy execute
  → Accepted/external_ref
  → status Wait/poll
  → health verification
  → succeeded 或 compensation/rollback
```

- “API 返回 200”不等于部署成功；必须验证目标 release 与健康状态。
- Rollback 是新的 ToolExecution，与原部署通过 causation ID 关联。
- 部署结果必须产生 ArtifactRef，至少包含环境、release、digest、外部记录 URI 和时间。

## 13. 错误模型

```rust
pub struct AdapterError {
    pub code: AdapterErrorCode,
    pub retry: AdapterRetryClass,
    pub safe_message: String,
    pub remote_request_id: Option<String>,
    pub retry_after: Option<Duration>,
    pub details: RedactedJson,
}

pub enum AdapterRetryClass {
    Never,
    SameRequestBackoff,
    ReconnectAndResume,
    QueryOutcome,
    ManualReview,
}
```

稳定错误码至少包括：

```text
AUTHENTICATION_FAILED
AUTHORIZATION_FAILED
ENDPOINT_UNAVAILABLE
RATE_LIMITED
REMOTE_TIMEOUT
REMOTE_REJECTED
PROTOCOL_MISMATCH
CAPABILITY_MISSING
INVALID_REMOTE_PAYLOAD
EVENT_CURSOR_INVALID
REMOTE_RUN_NOT_FOUND
SUBMISSION_UNCERTAIN
STOP_UNCERTAIN
OUTCOME_UNKNOWN
PAYLOAD_TOO_LARGE
POLICY_DENIED
```

- HTTP 5xx、连接重置等只有在尚未产生副作用或远程保证幂等时才能 SameRequestBackoff。
- HTTP timeout 不能自动等同于未提交。
- 远程错误 message 仅用于受控诊断；Runtime 分支使用稳定 code 和 retry class。

## 14. 网络、安全与凭据

- Endpoint 创建时执行协议、host、端口、TLS 和 SSRF 策略校验。
- 默认要求 TLS；明文连接只允许显式批准的 loopback/受控开发环境。
- 重定向不得跨越允许的 host/auth 边界，禁止把 Authorization 转发到未知目标。
- 连接池按 tenant/endpoint/credential identity 隔离，凭据轮换后旧连接必须失效。
- 限制请求、响应、单 Event、Event batch、附件和解压后大小。
- Adapter 必须验证 SSE/WS frame schema，未知事件按 forward-compatible 策略记录或忽略，不能直接推进状态。
- Webhook 必须验证签名、时间窗口和重放键，再提交 ApplyEvent。
- Artifact URI 和代码仓库凭据通过短期授权或 SecretRef 获取。
- 不允许第三方 Agent 默认访问整个平台凭据集合；按 Stage 和 Tool Policy 发放最小能力。

## 15. 可观测性

每次 Adapter 调用记录：

```text
adapter_kind, contract_version, endpoint_id, operation,
tenant_id, run_id, execution_id, remote_run_ref_hash,
attempt, latency, outcome, retry_class,
correlation_id, causation_id, remote_request_id
```

- 不记录明文 token、Authorization、完整 prompt、代码内容或未过滤工具输出。
- 指标至少包括 submit latency、uncertain submission、event lag、cursor replay、stop latency、schema rejection 和 capability mismatch。
- 远程 event occurred_at 与本地 recorded_at 分开记录，状态 deadline 使用本地数据库时间。

## 16. 官方 Adapter Profile 映射

### 16.1 OpenClaw Gateway

建议 profile：`openclaw-gateway-ws`。

| 规范化能力 | 官方协议映射 |
| --- | --- |
| probe | WebSocket `connect` / `hello-ok`，读取 protocol、features、policy 和 auth scopes |
| submit | Agent request，先 accepted ack，再等待最终响应 |
| events | Gateway `agent`、session/tool/approval 等事件 |
| status/wait | `agent.wait` 或 session/run 查询能力，按 hello features 验证 |
| stop | `sessions.abort`，尽量携带 runId 限定目标 |
| approvals | approval event + resolve RPC，需对应 operator scope |
| auth | Gateway token/device auth + operator scopes |

约束：

- 使用 `minProtocol/maxProtocol` 协商，不硬编码某个永久版本。
- `hello-ok.features` 是保守发现列表，不等于全部可调用方法；Adapter profile 仍需固定必需 method schema。
- accepted ack 不等于完成。
- `agent.wait` 必须受 EventReadLimits/deadline 约束；不能作为 Worker 的无限等待调用。
- Gateway 自身支持嵌入/子进程方式不改变本项目边界；Agent Loom 生产集成只连接已运行的远程 Gateway。
- 如果无法证明 submit 幂等或按 execution ID 查询，submission capability 标记为 Unsupported/RemoteReferenceOnly。

### 16.2 Hermes API Server

建议 profile：`hermes-runs-http`。

| 规范化能力 | 官方 API 映射 |
| --- | --- |
| probe | `GET /health`、`GET /v1/capabilities` |
| submit | `POST /v1/runs`，保存 run_id |
| status | `GET /v1/runs/{id}` |
| events | `GET /v1/runs/{id}/events` SSE |
| approval | `POST /v1/runs/{id}/approval` |
| guidance | `POST /v1/runs/{id}/steer` |
| stop | `POST /v1/runs/{id}/stop`，按 cooperative stop 处理 |
| session | session_id / previous_response_id，按 capability 验证 |

约束：

- stop 返回 stopping 只表示请求已接收，必须继续查询终态。
- steer accepted 表示排队，不表示 Agent 已消费；终态仍未消费的 guidance 需要映射回本地待处理输入。
- SSE 是否支持任意 cursor 续读必须以 capabilities 和实际协议为准；不支持时使用 status/result 对账。
- `/v1/chat/completions` 适合兼容客户端，但长期 Run Adapter 优先使用 Runs API。

### 16.3 OpenAI Responses / Codex-compatible

建议 profile：`openai-responses-background`。

| 规范化能力 | 官方 API 映射 |
| --- | --- |
| submit | `POST /responses`，使用 background 模式并保存 response ID |
| status/result | `GET /responses/{response_id}` |
| events | retrieve/stream，使用 starting-after sequence 续读 |
| stop | `POST /responses/{response_id}/cancel` |
| session continuity | conversation 或 previous_response_id，按请求契约选择 |
| tools | Responses tools/function/MCP 能力，按模型与 Endpoint capability 验证 |

该 profile 是官方 OpenAI API Adapter，可以承载编码类模型与工具，但不自动等价于完整 Codex 产品工作区、任务管理或本地 Codex harness。

如果 Endpoint 没有官方可验证的提交幂等或按 client execution ID 查询能力，submission idempotency 必须标为 `Unsupported`；创建请求超时后进入 reconcile/manual review，不能自动创建第二个 Response。

`codex-server` 作为独立 profile：

- 必须是可远程认证、版本协商、查询状态和恢复事件的 Server API；
- 在官方服务端协议、授权和生命周期契约可验证前标记 `experimental`；
- 不得通过 `spawn codex` CLI、解析终端输出或读取本地私有数据库实现生产 Adapter；
- 若未来官方协议提供 Run、event cursor、stop 和 artifact 语义，应通过新的 contract version 映射，不修改历史 capability snapshot。

## 17. Adapter 配置与版本

```rust
pub struct AdapterProfile {
    pub kind: AdapterKind,
    pub contract_version: AdapterContractVersion,
    pub protocol_range: ProtocolRange,
    pub required_capabilities: RequiredAgentCapabilities,
    pub endpoint_policy: EndpointPolicy,
    pub event_mapping_version: MappingVersion,
    pub error_mapping_version: MappingVersion,
}
```

- 配置变更创建新 profile/version，不原地改变历史映射。
- 事件和错误映射版本写入 AgentExecution。
- 新 Adapter 必须声明支持的 Runtime contract version range。
- 不兼容升级在 probe 阶段失败，不能等到执行中途才发现。
- Provider-specific feature flag 不得泄漏到 Workflow 通用状态机；Workflow 只声明规范化能力。

## 18. Conformance 测试

每个 AgentServerAdapter 必须通过：

1. probe 能识别兼容、不兼容、缺能力和认证失败。
2. submit accepted 不被映射为 completed。
3. 同一 idempotency key 的安全重试符合 capability 声明。
4. submit 响应丢失后进入 reconcile，不盲目创建第二个远程 Run。
5. Event batch 重复、乱序、断流和 cursor 恢复不重复推进本地状态。
6. Token delta 全部丢失不影响最终结果与状态机。
7. Stop accepted 后继续等待真实终态；stop/complete 竞争保留远程事实。
8. Approval 重复提交只产生一个远程决定。
9. Guidance queued 与 consumed 被正确区分。
10. Endpoint 升级和 capability 变化不会改写历史 Execution snapshot。
11. 凭据、prompt 和工具输出不会出现在日志或错误响应。
12. Worker 在 submit、event batch、stop 前后崩溃都可以通过 DurableStore 恢复。

每个 ToolAdapter 还必须通过：

1. 输入输出 schema 验证；
2. side-effect class 与重试策略一致；
3. idempotent write 重试复用相同 key；
4. non-idempotent write 超时进入 outcome_unknown；
5. query/compensation capability 与实际行为一致；
6. 长任务 Accepted 后释放 Worker，并通过 poll/Wait 恢复；
7. DevOps 成功以 release 与健康验证为准，不以 HTTP 200 为准。

建议为每个 Adapter 提供可控 Fake Server，支持注入重复事件、断流、超时、429/5xx、提交结果丢失、停止迟到和错误 payload，以运行相同 conformance suite。

## 19. 后续实现产物

```text
crates/adapter-core/src/agent.rs
crates/adapter-core/src/tool.rs
crates/adapter-core/src/capability.rs
crates/adapter-core/src/event.rs
crates/adapter-core/src/error.rs
crates/adapter-core/src/conformance/
```

下一份设计文档 `E2E_SCENARIO.md` 应使用本文规范化能力描述业产研交付，不在 Workflow 中直接写 OpenClaw/Hermes/OpenAI 私有状态或端点。
