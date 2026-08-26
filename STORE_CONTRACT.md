# Agent Loom DurableStore 契约与事务后置动作

## 1. 目的

本文基于 [STATE_MACHINE.md](./STATE_MACHINE.md) 和 [DOMAIN_MODEL.md](./DOMAIN_MODEL.md)，定义 Runtime 与 PostgreSQL/MySQL Provider 之间的正式领域契约，包括：

- Rust 公共接口形态；
- Command、Result、Error 和分页模型；
- 每个粗粒度操作的原子事务边界；
- Lease、幂等、版本和数据库时间语义；
- 事务提交后的可靠动作与可丢失提示；
- Provider 黑盒一致性测试要求。

本文不定义具体 SQL，也不允许 Runtime 通过通用 CRUD 绕过领域状态机。

## 2. Crate 边界

```text
crates/
├── domain/                # ID、状态、Command、Outcome、Error；不依赖 SQL 驱动
├── durable-store/         # DurableStore trait、测试套件、Provider capabilities
├── runtime/               # 状态规划、Worker/Scheduler/API 编排
├── store-postgres/        # PostgreSQL 事务与迁移
└── store-mysql/           # MySQL 事务与迁移
```

- `domain` 和 `durable-store` 不得导入 PostgreSQL/MySQL 驱动类型。
- Provider 返回领域错误，不向 Runtime 泄漏 SQLSTATE、错误号或方言 SQL。
- Runtime 不获得裸连接、事务句柄或任意 SQL 执行能力。
- Adapter 不直接调用 Provider CRUD；所有状态推进经 DurableStore 领域方法完成。
- 生产 Provider 必须由连接池实现 `DurableStore` 对象；一次方法调用只在内部借用一个连接，提交或查询完成后立即归还。连接获取失败统一映射为可重试的领域错误。

## 3. 公共基础类型

### 3.1 Future 与结果

为允许运行时持有动态 Provider，契约使用对象安全的 Future 返回形式。具体实现可以通过宏或手写 Future 实现，但公共语义等价于：

```rust
use std::{future::Future, pin::Pin};

pub type StoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = StoreResult<T>> + Send + 'a>>;

pub type StoreResult<T> = Result<T, StoreError>;

pub type DispatchFuture<'a> =
    Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
```

### 3.2 请求上下文

```rust
pub struct CommandContext {
    pub tenant_id: TenantId,
    pub actor: ActorRef,
    pub producer: ProducerRef,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<CausationId>,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
}
```

- 所有可改变状态或触发外部动作的命令必须携带 CommandContext。
- `request_hash` 在 Runtime/API 层基于规范化输入生成，Provider 必须与 CommandReceipt 比较。
- 查询使用独立 `QueryContext`，至少包含 tenant、actor 和授权后的可见范围。
- Provider 必须将 context 中的 tenant 与所有目标资源的 tenant 重新校验。

### 3.3 并发证明

```rust
pub struct ExpectedRun {
    pub run_id: RunId,
    pub version: Option<RunVersion>,
    pub execution_generation: Option<ExecutionGeneration>,
}

pub struct LeaseProof {
    pub task_id: TaskId,
    pub owner: WorkerId,
    pub token: LeaseToken,
}
```

- API 控制命令可以不传 expected version，但 Provider 仍必须锁定 Run 并验证当前状态。
- Worker 完成、失败和续租必须同时携带 Run generation 与 LeaseProof。
- Lease 是否过期只能由 Provider 使用数据库时间判断。

### 3.4 已提交结果

```rust
pub struct Committed<T> {
    pub value: T,
    pub commit: CommitInfo,
    pub hints: Vec<PostCommitHint>,
}

pub struct CommitInfo {
    pub committed_at: Instant,
    pub run_id: Option<RunId>,
    pub run_version: Option<RunVersion>,
    pub through_event_sequence: Option<EventSequence>,
}
```

Provider 只能在数据库事务成功提交后返回 `Committed<T>`。调用方收到成功结果时，结果中引用的 Run、Event、Checkpoint、Task 和 CommandReceipt 必须已经可从主库读取。

## 4. 可靠动作与事务后置提示

### 4.1 两类后置工作

事务产生的后续工作必须分为：

| 类型 | 示例 | 正确性要求 |
| --- | --- | --- |
| `DurableFollowUp` | 后续 Task、Timer、重试、Agent stop、对账、Outbox | 必须在业务事务内持久化，崩溃后可恢复 |
| `PostCommitHint` | 唤醒 Worker、唤醒 Scheduler、SSE 提示、缓存失效、指标 | 可以丢失，不得影响最终正确性 |

禁止把以下动作只实现为内存 after-commit callback：

- Pause/Cancel 后停止远程 Agent；
- Tool/Agent `outcome_unknown` 对账；
- 延迟重试与 deadline；
- Webhook/审批恢复；
- 必须送达的外部事件发布；
- DevOps 部署补偿或状态确认。

这些动作必须先转换为 Task、WaitSubscription、Execution 状态或 OutboxMessage。PostCommitHint 只负责降低发现延迟。

### 4.2 DurableFollowUp

```rust
pub enum FollowUpKind {
    ExecuteTask,
    WakeTimer,
    StopAgentExecution,
    ReconcileAgentExecution,
    ReconcileToolExecution,
    CompensateToolExecution,
    PublishOutbox,
}
```

DurableFollowUp 不要求单独一张通用表：

- 执行、停止、对账和补偿可以表示为类型化 Task；
- Timer 和重试通过 Task `available_at` 或 WaitSubscription `expires_at` 表达；
- 消息发布通过 OutboxMessage 表达。

无论采用哪种物理形式，都必须具有稳定 logical key、幂等键、状态、attempt 和可恢复时间。

### 4.3 PostCommitHint

```rust
pub enum PostCommitHint {
    WakeWorkers { queue: QueueKey },
    WakeScheduler { shard: SchedulerShard },
    RunEventsAvailable {
        tenant_id: TenantId,
        run_id: RunId,
        through_sequence: EventSequence,
    },
    InvalidateRunCache { tenant_id: TenantId, run_id: RunId },
    OutboxAvailable,
}
```

约束：

- Hint 不携带凭据、完整 Checkpoint 或敏感 Event payload。
- Hint 执行失败只记录日志/指标，不得把已提交业务事务报告为失败。
- Worker/Scheduler 必须周期性扫描 DurableStore，因此 Hint 永久丢失也只影响延迟。
- SSE 收到 Hint 后从 Event Store 按 sequence 拉取，不直接把 Hint 当成事件内容。
- Provider 可以不支持数据库通知；此时返回 Hint 仍可由进程内 dispatcher 使用。

### 4.4 提交后执行器

```rust
pub trait PostCommitDispatcher: Send + Sync {
    fn dispatch<'a>(&'a self, hints: Vec<PostCommitHint>) -> DispatchFuture<'a>;
}
```

Runtime 调用顺序：

```text
result = store.command(...).await       # 此时事务已提交
respond_or_continue(result.value)
dispatcher.dispatch(result.hints)       # best effort，可并行
```

API 可以在调度 Hint 前返回已提交结果。若进程在 commit 后、dispatch 前崩溃，Scheduler/Worker 轮询和 Event Store 补读必须完成恢复。

## 5. DurableStore Trait

下面的接口名称和字段是 Phase 0 基线。实现前可以做不改变领域语义的 Rust 命名调整。

```rust
pub trait DurableStore: Send + Sync {
    fn capabilities(&self) -> StoreCapabilities;

    // Definitions
    fn create_workflow<'a>(
        &'a self,
        cmd: CreateWorkflow,
    ) -> StoreFuture<'a, Committed<WorkflowSnapshot>>;

    fn publish_workflow_version<'a>(
        &'a self,
        cmd: PublishWorkflowVersion,
    ) -> StoreFuture<'a, Committed<WorkflowVersionSnapshot>>;

    fn create_agent_definition<'a>(
        &'a self,
        cmd: CreateAgentDefinition,
    ) -> StoreFuture<'a, Committed<AgentDefinitionSnapshot>>;

    fn upsert_agent_endpoint<'a>(
        &'a self,
        cmd: UpsertAgentEndpoint,
    ) -> StoreFuture<'a, Committed<AgentEndpointSnapshot>>;

    // Run lifecycle
    fn create_run<'a>(
        &'a self,
        cmd: CreateRun,
    ) -> StoreFuture<'a, Committed<CreatedRun>>;

    fn pause_run<'a>(
        &'a self,
        cmd: PauseRun,
    ) -> StoreFuture<'a, Committed<CommandOutcome<RunSnapshot>>>;

    fn resume_run<'a>(
        &'a self,
        cmd: ResumeRun,
    ) -> StoreFuture<'a, Committed<CommandOutcome<RunSnapshot>>>;

    fn cancel_run<'a>(
        &'a self,
        cmd: CancelRun,
    ) -> StoreFuture<'a, Committed<CommandOutcome<RunSnapshot>>>;

    // Task queue and worker completion
    fn claim_task<'a>(
        &'a self,
        cmd: ClaimTask,
    ) -> StoreFuture<'a, Option<Committed<LeasedTask>>>;

    fn renew_lease<'a>(
        &'a self,
        cmd: RenewLease,
    ) -> StoreFuture<'a, Committed<LeaseSnapshot>>;

    fn complete_task<'a>(
        &'a self,
        cmd: CompleteTask,
    ) -> StoreFuture<'a, Committed<TaskCompletion>>;

    fn fail_task<'a>(
        &'a self,
        cmd: FailTask,
    ) -> StoreFuture<'a, Committed<TaskFailureOutcome>>;

    // External events and waits
    fn apply_event<'a>(
        &'a self,
        cmd: ApplyEvent,
    ) -> StoreFuture<'a, Committed<ApplyEventOutcome>>;

    // Tool and remote Agent executions
    fn prepare_tool_execution<'a>(
        &'a self,
        cmd: PrepareToolExecution,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>>;

    fn begin_tool_retry_attempt<'a>(
        &'a self,
        cmd: BeginToolRetryAttempt,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>>;

    fn record_tool_outcome<'a>(
        &'a self,
        cmd: RecordToolOutcome,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>>;

    fn prepare_agent_execution<'a>(
        &'a self,
        cmd: PrepareAgentExecution,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>>;

    fn begin_agent_resubmission<'a>(
        &'a self,
        cmd: BeginAgentResubmission,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>>;

    fn record_agent_submission<'a>(
        &'a self,
        cmd: RecordAgentSubmission,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>>;

    fn append_agent_events<'a>(
        &'a self,
        cmd: AppendAgentEvents,
    ) -> StoreFuture<'a, Committed<AgentEventBatchOutcome>>;

    fn record_agent_outcome<'a>(
        &'a self,
        cmd: RecordAgentOutcome,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>>;

    // Scheduler
    fn scan_due_work<'a>(
        &'a self,
        query: DueWorkQuery,
    ) -> StoreFuture<'a, DueWorkPage>;

    fn apply_due_work<'a>(
        &'a self,
        cmd: ApplyDueWork,
    ) -> StoreFuture<'a, Committed<DueWorkOutcome>>;

    // Authoritative queries
    fn get_run<'a>(
        &'a self,
        query: GetRun,
    ) -> StoreFuture<'a, Option<RunSnapshot>>;

    fn list_events<'a>(
        &'a self,
        query: ListEvents,
    ) -> StoreFuture<'a, EventPage>;

    fn list_stages<'a>(
        &'a self,
        query: ListStages,
    ) -> StoreFuture<'a, StagePage>;

    fn list_artifacts<'a>(
        &'a self,
        query: ListArtifacts,
    ) -> StoreFuture<'a, ArtifactPage>;

    fn get_command_receipt<'a>(
        &'a self,
        query: GetCommandReceipt,
    ) -> StoreFuture<'a, Option<CommandReceiptSnapshot>>;

    fn health<'a>(&'a self) -> StoreFuture<'a, StoreHealth>;
}
```

## 6. Command 模型

### 6.1 CreateRun

```rust
pub struct CreateRun {
    pub context: CommandContext,
    pub run_id: RunId,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub coordinator_agent_version_id: Option<AgentVersionId>,
    pub parent: Option<ParentRunRef>,
    pub input: RedactedJson,
    pub deadline: Option<Instant>,
    pub initial_plan: InitialRunPlan,
}
```

`initial_plan` 是已经过 Runtime 验证的类型化计划，包含初始 Stage、Task 和 Artifact Contract。Provider 必须再次验证引用、tenant 和唯一 logical key。

### 6.2 CompleteTask

```rust
pub struct CompleteTask {
    pub context: CommandContext,
    pub expected_run: ExpectedRun,
    pub lease: LeaseProof,
    pub checkpoint: NewCheckpoint,
    pub task_result: TaskResult,
    pub stage_mutation: Option<StageMutation>,
    pub artifacts: Vec<NewArtifactRef>,
    pub next: NextActions,
}

pub enum NextActions {
    Tasks(Vec<NewTask>),
    Wait(NewWaitSubscription),
    Retry(NewRetrySchedule),
    FinishRun(FinalRunResult),
    NoFurtherWork,
}

pub struct NewWaitSubscription {
    pub wait_id: WaitId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub wait_type: String,
    pub expected_event_type: String,
    pub match_key_hash: Digest,
    pub match_contract: RedactedJson,
    pub expires_at: Option<Instant>,
    pub resume_task: WaitResumeTask,
    pub created_event_id: EventId,
}

pub struct WaitResumeTask {
    pub task_id: TaskId,
    pub logical_key: LogicalKey,
    pub kind: TaskKind,
    pub priority: i32,
    pub max_attempts: u32,
    pub input: RedactedJson,
    pub deadline: Option<Instant>,
}
```

Provider 不执行 Agent 规划，但必须验证 `next` 与当前状态机兼容，例如：

- FinishRun 时所有必需 Stage 和 Artifact Contract 已满足；
- Wait 时当前 Worker 可以释放且 Wait logical key 唯一；
- 新 Task generation 等于 Run generation；
- 终态、paused 或过期 Run 不接受普通 completion。

### 6.3 ApplyEvent

```rust
pub struct ApplyEvent {
    pub expected_run: ExpectedRun,
    pub event_id: EventId,
    pub event_type: String,
    pub match_key_hash: Digest,
    pub payload_schema_version: SchemaVersion,
    pub payload: RedactedJson,
    pub signature_verification: SignatureVerification,
    pub occurred_at: Option<Instant>,
}
```

Provider 必须匹配并消费 WaitSubscription。恢复 Task 计划在 Wait 创建时持久化，调用方不得在事件到达时临时指定任意下一状态或绕过等待契约。签名校验失败的事件不得进入状态事务；`NotRequired` 只用于已经处于受信边界内的事件源。

基础 `match_contract` 是可移植 JSON 对象：`required` 为 payload 必须包含的字段名数组，`equals` 为字段精确值约束。Provider 必须至少实现这两项；更复杂的 Schema 校验应在 Runtime 规范化层完成，并将稳定的校验结论随命令传入。

### 6.4 DueWork

```rust
pub enum DueWorkKind {
    ExpiredLease,
    DueRetry,
    ExpiredWait,
    RunDeadline,
    StaleAgentSubmission,
    StaleToolExecution,
    PendingOutbox,
}
```

`scan_due_work` 是只读候选扫描，不代表候选仍有效。`apply_due_work` 必须重新锁定相关 Run/Task/Wait/Execution，并以当前数据库时间再次校验。

Tool/Agent retry 候选使用数据库 `retry_at <= db_now` 判断，并按 `(due_at, kind, execution_id)` 做稳定 keyset 分页。候选携带 Execution revision、Run fence、checkpoint sequence 和 `stage_execution_id`，使 Runtime 能构造归属明确的恢复 Task；revision 只用于后续 CAS，扫描本身不加跨请求锁，也不消费 retry。

`apply_due_work` 对外部 retry 必须按 Run → Execution 锁序重新验证 Run version/generation/checkpoint、Execution revision/status、原始 `retry_at` 与数据库当前时间。获胜事务消费 `retry_at`、追加 due Event、创建 `reconcile` Task 并推进 Run；暂停 Run 创建 `scheduled` Task，恢复时再转为 queued。重复命令通过 Receipt 返回原结果，不生成第二个恢复 Task。

## 7. 原子操作契约

| 操作 | 同一事务必须完成 | 典型 DurableFollowUp | PostCommitHint |
| --- | --- | --- | --- |
| `create_run` | Receipt、Run、初始 Stage/Task、Checkpoint、`run.created` | 初始 Task | WakeWorkers、RunEventsAvailable |
| `claim_task` | Run/Task 校验、Lease、TaskAttempt、必要的 `run.running` | 无 | RunEventsAvailable |
| `complete_task` | Receipt、Lease/版本校验、Task、Stage、Artifact、Checkpoint、Event、后续工作、Run 投影 | Task/Wait/Timer/Outbox | WakeWorkers/Scheduler、SSE、缓存失效 |
| `fail_task` | Receipt、TaskAttempt、错误分类、retry/dead-letter、Run 投影、Event | Retry/人工恢复 | WakeScheduler、SSE |
| `apply_event` | Receipt、Wait 消费、Event、Checkpoint/Stage、恢复 Task、Run 投影 | 恢复 Task | WakeWorkers、SSE |
| `pause_run` | Receipt、Run/generation、Checkpoint 指针、暂停 Event、停止意图 | Agent/Tool stop Task | WakeWorkers/Scheduler、SSE |
| `resume_run` | Receipt、未知结果检查、Checkpoint 兼容性、恢复 Task、Run/Event | 恢复/对账 Task | WakeWorkers、SSE |
| `cancel_run` | Receipt、唯一终态、Wait 关闭、Task 控制门、Event | Agent/Tool stop 或补偿 Task | WakeScheduler、SSE |
| `prepare_*_execution` | Receipt、Execution 意图、状态、Task 关联 Event | 执行 Task 自身 | 无或 SSE |
| `begin_tool_retry_attempt` | Receipt、恢复 Task Lease/Run fence/来源 Event 校验、Execution revision、attempt 递增、未完成 attempt、Event | Tool 外部调用 | 无或 SSE |
| `begin_agent_resubmission` | Receipt、恢复 Task Lease/Run fence/来源 Event 校验、Execution version、submitting 状态、Event | Agent 外部提交 | 无或 SSE |
| `record_*_outcome` | Receipt、Execution 结果、Event、Checkpoint/Task/Run 推进 | 对账/补偿/后续 Task | WakeWorkers、SSE |
| `apply_due_work` | 当前时间与状态复核、Event、状态推进、后续 Task | 重试/对账/停止 | WakeWorkers/Scheduler、SSE |

任何操作都不得先 commit 状态，再通过内存后置动作补写必须存在的 Task、Wait、Checkpoint 或 Event。

## 8. Tool 与 Agent 外部调用窗口

具体远程协议、能力匹配、规范化事件和 Adapter 错误模型以 [ADAPTER_CONTRACT.md](./ADAPTER_CONTRACT.md) 为准；DurableStore 只接收规范化结果。

### 8.1 Tool

```text
prepare_tool_execution（提交 executing 意图）
  → commit
  → Tool Adapter 外部调用
  → record_tool_outcome（提交 succeeded/failed/outcome_unknown）
```

Worker 在外部调用后、本地记录前崩溃时，Scheduler 根据 stale executing 生成持久化 reconcile Task。只有 Adapter 能证明幂等重放安全时才能重发调用。

`SameRequestBackoff` 结果必须携带 `retry_at`，并与 ToolExecution 的 `retry_scheduled` 状态同事务持久化；其他结果不得残留 retry time。

到期重试由 `apply_due_work` 创建的 `reconcile` Task 承载。Worker 领取后必须先调用 `begin_tool_retry_attempt`；Provider 必须验证有效 Lease、Run version/generation、Execution revision，并确认 Task 的 `created_event_id` 指向同一 ToolExecution 的 `tool.retry_due` Event。获胜事务将 Execution 从 `reconciling` 置为 `executing`、递增 attempt、插入新的未完成 ToolExecutionAttempt、追加 `tool.retry_attempt_started` 并保存 Receipt。只有该事务提交后才能调用 Tool Adapter。

### 8.2 Agent Server

```text
prepare_agent_execution（提交 submitting 意图）
  → commit
  → AgentServerAdapter.submit
  → record_agent_submission（保存 remote_run_ref、Wait/poll）
```

`submitting` 长时间未保存 remote reference 时必须进入 reconcile，而不是重新 submit。Pause/Cancel 事务把 Execution 标记为 stop requested 并创建稳定 logical key 的 stop Task；PostCommitHint 只唤醒 Worker。

Agent 提交拒绝使用与 Tool 相同的 `ExecutionRetryClass`。`SameRequestBackoff` 必须携带 `retry_at` 并投影为 `reconciling`；其他分类不得携带 retry time。`record_agent_submission` 即使发现 Run version/generation 已变化，也必须保存外部提交证据，但不得借此推进已被 fencing 的业务状态。

Agent 到期重提同样只能由匹配 `agent.retry_due` Event 创建且已被当前 Worker 领取的 `reconcile` Task 发起。`begin_agent_resubmission` 必须在一个事务内验证 Lease、Run fence、Execution version、`reconciling` 状态、已消费的 retry time 和尚无 `remote_run_ref`，再将 Execution 置为 `submitting`、递增 version、追加 `agent.resubmission_started` 并保存 Receipt。重提沿用原 Endpoint、session reference 和 idempotency key；事务提交后才调用 Agent Server，随后仍由 `record_agent_submission` 保存远端证据。

### 8.3 远程事件批次

`append_agent_events` 必须在一个事务中：

1. 按全局锁序先锁定 Run，再锁定 AgentExecution 并校验 cursor version；
2. 在 AgentEventReceipt 守卫中按远程 event ID 或规范化幂等键检查重复和 raw digest 冲突；
3. 对新权威事件预生成本地 Event ID，先追加 Event，再插入引用它的 AgentEventReceipt；transient/ignored 事件只插入无 local Event 的 receipt；
4. 更新持久化 cursor；
5. 根据事件创建 Wait/Task/Artifact 或完成 Execution；
6. 保存 Receipt 并提交。

cursor 更新失败时整批回滚，客户端可以安全从旧 cursor 重读。

底层批次事务只接受已规范化事件并维护 receipt/Event/cursor 原子性。每个权威事件可携带显式 `AgentEventProjection { workflow_action, artifacts, execution_outcome }`；将具体 `event_kind` 派生为这些字段的规则属于 Runtime 投影层，禁止 Provider 根据 vendor payload 临时推断。

投影必须满足：transient/ignored 事件无投影；Task/Wait/Artifact 的 `created_event_id` 等于所属本地 Event；一个批次至多有一个调度决策和一个 Execution outcome。Provider 对新 receipt 执行投影，对 duplicate receipt 跳过投影；Run fence 失效时只保存外部事实和 cursor，不创建业务 Task/Wait/Artifact。

## 9. Scheduler 契约

- Scheduler 可以多实例并发运行，不拥有唯一业务状态。
- `scan_due_work` 返回带游标的有限候选页，不锁定对象跨请求等待。
- 每个候选通过 `apply_due_work` 单独、短事务处理。
- Scheduler 批次中一个候选失败不得回滚其他候选。
- 同一候选的 Command、Event、恢复 Task 与 idempotency key 必须可确定性重建，Scheduler 在扫描后、应用前崩溃时可以安全重扫。
- 暂时性数据库错误使用有限指数退避；状态冲突视为其他实例已处理。
- Scheduler 必须覆盖以下兜底扫描：过期 Lease、到期 retry、Wait timeout、Run deadline、stale external execution、未发布 Outbox。

数据库通知或 Hint 只能提前触发扫描，不能替代周期性兜底扫描。

## 10. 查询与分页

### 10.1 一致性级别

```rust
pub enum ReadConsistency {
    Authoritative,
    StaleOk { max_lag: Duration },
}
```

- 状态推进前的读取、幂等检查、Lease 和控制命令只能使用 Authoritative。
- 读副本只服务显式 `StaleOk` 查询。
- 如果 Provider 无法证明副本延迟不超过 `max_lag`，必须回退主库或拒绝该一致性请求。

### 10.2 Cursor

- Event cursor 包含 run_id 和最后 sequence，按 `sequence > after` 查询。
- 普通列表 cursor 使用排序字段和唯一 ID，例如 `(updated_at, run_id)`。
- Cursor 是不透明、版本化并带完整性校验的值；客户端不得拼接 SQL 排序字段。
- 分页必须使用 keyset pagination，不使用高 offset 作为权威事件查询方案。
- commit 结果未知时，调用方可以使用原 tenant、scope 和 idempotency key 查询 CommandReceipt；该查询必须访问主库。

## 11. 错误模型

```rust
pub struct StoreError {
    pub code: StoreErrorCode,
    pub retry: RetryClass,
    pub message: SafeMessage,
    pub context: ErrorContext,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

pub enum RetryClass {
    Never,
    ReloadState,
    Backoff,
    Reconcile,
}
```

公共错误码至少包括：

```text
NOT_FOUND
TENANT_MISMATCH
INVALID_TRANSITION
VERSION_CONFLICT
TERMINAL_RUN
LEASE_LOST
LEASE_EXPIRED
IDEMPOTENCY_KEY_REUSED
WAIT_MISMATCH
WAIT_ALREADY_CONSUMED
WAIT_EXPIRED
DEADLINE_EXCEEDED
OUTCOME_UNKNOWN
PAUSE_RECOVERY_REQUIRED
ADAPTER_CAPABILITY_MISSING
INCONSISTENT_PROJECTION
CONSTRAINT_VIOLATION
SERIALIZATION_CONFLICT
STORE_UNAVAILABLE
MIGRATION_REQUIRED
```

- Provider 将死锁、序列化失败、连接中断等映射到稳定 RetryClass。
- API 不返回原始 SQL、表名、连接地址或包含 payload 的数据库错误。
- 事务在 commit 结果未知时必须返回 `OUTCOME_UNKNOWN`；调用方以 CommandReceipt 查询首次结果，不得直接假设失败并生成新幂等键。

## 12. Provider Capabilities

```rust
pub struct StoreCapabilities {
    pub wakeup_notification: bool,
    pub read_replica: bool,
    pub json_path_query: bool,
    pub partition_management: bool,
    pub full_text_search: bool,
}
```

- Capabilities 只影响性能和可选查询。
- DurableStore 基础方法不能因 capability 为 false 而失去正确性。
- `wakeup_notification = false` 时 Worker/Scheduler 使用轮询。
- Runtime 不通过数据库类型判断 Provider 行为，只读取 capability。

## 13. 事务重试

- Provider 可以对死锁和序列化冲突执行有限内部重试。
- 只有不包含外部调用、输入完整且幂等的领域事务可以内部重试。
- 重试必须复用同一 CommandContext、ID、logical key 和 request hash。
- Provider 不得在事务重试闭包中调用 LLM、HTTP、MCP、Agent Server 或 DevOps API。
- 超过内部重试上限后返回 `SERIALIZATION_CONFLICT + Backoff`。
- commit 返回网络错误且是否成功未知时，不执行新事务；先按 CommandReceipt 查询结果。

## 14. 安全与可观测性

- 每个 Provider span 包含 operation、tenant、run、task、attempt、correlation 和数据库耗时。
- 日志不得记录 LeaseToken 原文、credential_ref 对应密钥、完整 Checkpoint 或未过滤 payload。
- 慢事务、死锁重试、Lease 冲突、CommandReceipt 命中和 outcome_unknown 必须有指标。
- QueryContext 的授权结果由 API/Runtime 提供，但 Provider 仍强制 tenant 条件。
- 健康检查不得执行业务写入；迁移状态和主库可写性分别报告。

## 15. Provider 一致性测试

每个生产 Provider 必须通过同一套 `durable-store` 黑盒测试：

1. 每个 trait 方法验证 tenant 隔离和 CommandReceipt 幂等。
2. `create_run` 不出现“Run 已建但首 Task/Event 缺失”。
3. 并发 claim 只有一个有效 Lease。
4. renew/reclaim/complete 三方竞争符合 Lease token 和数据库时间语义。
5. complete/cancel/pause/deadline 只产生合法状态和唯一 terminal Event。
6. Wait 匹配、消费、超时和 Pause 期间事件处理均为单次原子操作。
7. Tool/Agent 外部调用窗口进入可恢复的 outcome_unknown，不自动重复副作用。
8. Agent event batch 的本地 Event 与 cursor 同进同退。
9. PostCommitHint 全部丢失时，轮询仍完成 Task、Timer、stop、reconcile 和 Outbox。
10. Hint dispatcher 报错时，已提交命令仍返回成功且状态可查询。
11. commit 响应丢失后，以相同幂等命令重试返回首次结果。
12. PostgreSQL/MySQL 对同一命令历史返回等价 Outcome、ErrorCode、Run 状态和 Event 顺序。

## 16. 待实现文件

实现阶段建议从以下文件开始：

```text
crates/domain/src/store/command.rs
crates/domain/src/store/outcome.rs
crates/domain/src/store/error.rs
crates/durable-store/src/lib.rs
crates/durable-store/src/conformance/
crates/store-postgres/migrations/
crates/store-mysql/migrations/
```

正式编码前仍需在 Rust workspace 中验证 trait 的对象安全、Send 边界、错误 source 生命周期和序列化格式，但不得改变本文规定的事务及后置动作语义。

Provider 的物理类型、迁移批次、锁/CAS SQL 形状和在线 schema 演进规则以 [MIGRATION_DESIGN.md](./MIGRATION_DESIGN.md) 为准。
