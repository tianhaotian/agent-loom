# Agent Loom 技术实施计划

## 1. 项目定位

Agent Loom 是面向服务端 Agent 的事件驱动、持久化运行时。它将 Agent 的一次长期执行抽象为可审计、可暂停、可恢复的 `Run`，并用事件、任务与 checkpoint 使任何服务实例都可以在故障或扩缩容后继续执行。

首要目标不是实现某一个模型或工具 SDK，而是构建正确的 Durable Runtime 内核：状态不依赖进程内存，副作用可追踪，多个 Worker 可安全协作。

产品落点是复杂流程编排和 Agent 平台团队协作。首个垂直场景为“业产研交付”：需求分析与澄清、原型/PRD、技术方案、编码、自测、集成测试、DevOps 部署。流程允许人和多个专业 Agent 协作、动态拆解 Task、产物交接、人工门禁、失败返工和跨服务故障恢复。

## 2. 已确定的架构决策

### 2.1 服务与运行模型

- 主语言：Rust。
- 服务模型：API、Scheduler、Worker 全部无状态并可水平扩展。
- Worker 一次只执行一个有限的 Turn 或 Task；不得在进程中等待审批、回调或定时器。
- 运行时的唯一权威状态保存在持久化存储中；进程内存仅可作为可丢失缓存。
- 一致性语义：至少一次执行（at-least-once）加幂等副作用，不承诺跨外部系统的 exactly-once。
- 第三方 Agent 通过 Server/Gateway/API Adapter 接入。生产模式不由 Runtime spawn 或监管本地 Agent 子进程。
- Workflow Definition 提供版本化流程骨架，但 Runtime 允许根据上下文动态生成 Task、分支、返工与 Handoff，不强制固定 DAG。

### 2.2 存储模型

- 首个参考 Provider：PostgreSQL。
- 对等生产 Provider：MySQL 8+ / InnoDB。
- PostgreSQL 与 MySQL 是 `DurableStore` 的实现，而非 Agent Tool Plugin。
- PostgreSQL 与 MySQL 都是明确支持的生产目标，并共享同一领域契约、逻辑模型和一致性测试。PostgreSQL 仅作为首个参考实现，不构成 Runtime 语义上的特殊依赖。
- SQLite 仅用于本地开发或测试；Redis 不作为权威存储。
- 初期不引入消息队列；持久化任务表与短间隔轮询是正确性基础。后续可选接入 NATS/Kafka 等分发层。

### 2.3 推荐部署形态

```text
API / Webhook / SSE instances
             │
             ▼
      DurableStore Provider
  (PostgreSQL or MySQL / InnoDB)
             │
   ┌─────────┴──────────┐
   ▼                    ▼
Scheduler instances   Worker instances
  timeout / lease       turn / tool task
```

所有实例可被任意重启或扩缩容。任务被某个实例领取的事实、Lease 的有效期和 Run 的当前状态必须由 DurableStore 决定。

## 3. 领域模型

### 3.1 核心实体

字段、关系、索引与 PostgreSQL/MySQL 共享逻辑类型以 [DOMAIN_MODEL.md](./DOMAIN_MODEL.md) 为准。

| 实体 | 作用 | 关键字段 |
| --- | --- | --- |
| `WorkflowDefinition` | 版本化流程骨架与门禁 | workflow_id、version、stages、roles、artifact_contracts |
| `StageExecution` | 业务阶段的一次执行与返工记录 | stage_execution_id、run_id、stage_key、status、assignee、attempt、parent_stage_id |
| `AgentDefinition` | Agent 静态配置 | agent_id、version、system_prompt、tools、limits |
| `Run` | 长期执行的当前状态投影 | run_id、status、version、input、deadline、checkpoint_id |
| `Event` | 不可变领域事实与审计记录 | event_id、run_id、type、payload、idempotency_key、causation_id、correlation_id |
| `CommandReceipt` | 命令和事件的确定性幂等结果 | tenant_id、scope、idempotency_key、request_hash、outcome |
| `Task` | 可独立领取与重试的工作单元 | task_id、run_id、kind、status、available_at、attempt、lease |
| `Checkpoint` | 恢复所需上下文 | checkpoint_id、run_id、sequence、schema_version、definition_versions、state |
| `WaitSubscription` | 外部恢复条件 | wait_id、run_id、type、match_key、status、expires_at、consumed_by_event_id |
| `ArtifactRef` | 交付物版本与外部引用 | artifact_id、run_id、task_id、kind、version、uri、digest |
| `ToolExecution` | 外部副作用的幂等执行记录 | tool_call_id、idempotency_key、status、result、error、recovery_action |
| `AgentEndpoint` | 远程 Agent Server 配置与能力 | endpoint_id、adapter_kind、protocol_version、capabilities、auth_ref |
| `AgentExecution` | 远程 Agent Run 的持久化映射 | execution_id、endpoint_id、remote_run_ref、idempotency_key、status、event_cursor |

### 3.2 Run 状态机

```text
queued --task.claimed--> running --next_task.created--> queued
                            ├--wait.created----------> waiting / approval_required
                            ├--retry.scheduled-------> retrying
                            └--run.succeeded---------> completed
waiting / approval_required --matching_event-------> queued
retrying --retry.due-------------------------------> queued
任意非终态 --pause-------------------------------> paused
paused --resume/revalidate------------------------> queued / 原等待态
任意非终态 --cancel / deadline / fatal-----------> cancelled / timed_out / failed
```

- `waiting`：等待外部回调、定时器或子 Run。
- `approval_required`：等待满足固定输入契约的人工审批事件。
- `retrying`：已安排重试，但还没有开始新的执行。
- `completed`、`failed`、`cancelled`、`timed_out` 是终态；普通事件不得使终态重新进入执行态。

### 3.3 状态转换与竞争规则

- 所有 Run 状态推进通过数据库事务中的行锁或 CAS/条件更新完成，条件至少包含 `run_id + current_status + version`。
- `cancel` 与 `complete_task` 采用数据库串行化后的首个合法提交获胜。完成先提交时取消返回终态不变；取消先提交时迟到完成被拒绝，不能覆盖 `cancelled`、checkpoint 或后续 Task。
- `pause` 的控制面语义是立即生效：事务提交后冻结未开始 Task、禁止领取或生成可执行 Task，但允许记录被暂停门阻塞的恢复 Task；同时向正在执行的 Agent Server Adapter 发送尽力停止。无法撤回的外部副作用进入 ToolExecution 恢复流程。
- 暂停保留最后一个成功 checkpoint、最近成功用户消息、已确认 ArtifactRef 和未执行的动态 Task 计划。恢复操作基于该提交点重新调度，而不是依赖 Worker 内存。
- 重复命令返回第一次提交的等价结果；非法、不匹配、过期和乱序事件仅写结构化错误及可选 rejected/ignored 审计事件，不改变 Run 状态。
- [STATE_MACHINE.md](./STATE_MACHINE.md) 已建立完整语义基线；Phase 0 必须评审并冻结其中的转换矩阵，包括 cancel/complete、pause/complete、timeout/complete、approval/reject 和重复事件竞争。

## 4. DurableStore 插件契约

正式 Rust 接口、原子操作、错误模型与事务后置动作以 [STORE_CONTRACT.md](./STORE_CONTRACT.md) 为准。

### 4.1 设计原则

禁止暴露通用 KV 或 CRUD 接口作为运行时基础。Runtime 只调用表达领域意图的粗粒度操作；Provider 在内部将它们实现为单一数据库事务。

```rust
trait DurableStore {
    async fn create_workflow(&self, command: CreateWorkflow) -> Result<WorkflowSnapshot>;
    async fn create_run(&self, command: CreateRun) -> Result<CreatedRun>;
    async fn get_stage(&self, stage_id: StageExecutionId) -> Result<Option<StageSnapshot>>;
    async fn apply_event(&self, command: ApplyEvent) -> Result<ApplyEventResult>;
    async fn claim_task(&self, command: ClaimTask) -> Result<Option<LeasedTask>>;
    async fn renew_lease(&self, command: RenewLease) -> Result<Lease>;
    async fn complete_task(&self, command: CompleteTask) -> Result<Completion>;
    async fn fail_task(&self, command: FailTask) -> Result<FailureOutcome>;
    async fn request_pause(&self, command: PauseRun) -> Result<CommandOutcome>;
    async fn resume_run(&self, command: ResumeRun) -> Result<CommandOutcome>;
    async fn cancel_run(&self, command: CancelRun) -> Result<CommandOutcome>;
    async fn get_run(&self, run_id: RunId) -> Result<Option<RunSnapshot>>;
    async fn list_events(&self, query: EventQuery) -> Result<EventPage>;
}
```

接口名称将在实现前细化，但以下语义属于稳定契约。

### 4.2 必须保证的原子事务

`complete_task` 必须在一个数据库事务中完成：

1. 校验 Task 状态、Lease token、Lease owner 与未过期时间；
2. 校验 Run 当前版本与合法状态转换；
3. 写入新的 Event（含幂等去重）；
4. 写入新的 Checkpoint；
5. 更新 Run 状态投影和版本号；
6. 将当前 Task 标记为完成；
7. 创建后续 Task、等待订阅或定时唤醒记录；
8. 提交事务。

任一环节失败必须回滚。`fail_task`、`apply_event`、`cancel`、`pause`、`resume` 和 `approval.received` 同样遵循这个原则。

等待事件的处理事务必须原子完成 WaitSubscription 匹配与消费、幂等记录、Event 追加、Run/Checkpoint 更新和恢复 Task 创建。ArtifactRef 的登记必须与产生该产物的 Task/Event 建立不可歧义的关联。阶段完成事务还必须原子更新 StageExecution、登记阶段产物，并创建质量门禁或后续阶段。

### 4.3 并发与 Lease

- Worker 使用 `SELECT ... FOR UPDATE SKIP LOCKED` 领取可运行任务。
- 领取操作在短事务内把 Task 置为 `leased`，写入 `lease_owner`、随机 `lease_token` 与 `lease_expires_at`。
- 完成或续租必须同时匹配 Task ID、Lease token 和 owner，防止失效 Worker 覆盖新 Worker 的结果。
- Scheduler 回收过期 Lease；回收本身必须幂等。
- 对同一 Run 的状态变更采用行锁或乐观版本号；禁止两个事件并发生成相互矛盾的 checkpoint。
- 事件序列以 `(run_id, sequence)` 唯一约束保证单调顺序；外部输入幂等约束至少覆盖 tenant、run、producer 和 idempotency key。
- 所有 Lease、deadline 与延迟任务判断使用数据库权威时间，避免 Worker 节点时钟漂移改变状态结果。

### 4.4 幂等与外部副作用

- 每个外部输入事件必须有稳定的 `idempotency_key`；数据库以唯一约束去重。
- 每个 Tool Call 必须在 `tool_executions` 中保存稳定的 `tool_call_id` 与 idempotency key。
- 重试前先查询既有 `ToolExecution`；已有成功结果时复用，不重新执行副作用。
- 对接外部 API 时传递同一幂等键；不支持幂等的系统需实现查询、补偿或人工介入策略。
- 外部调用不在数据库事务内执行。数据库只能保证内部原子性，不能制造跨 HTTP/LLM/MCP 的分布式事务。
- ToolExecution 使用 `planned → executing → succeeded/failed/outcome_unknown` 状态；Worker 在外部成功后、本地提交前崩溃时，不得盲目重试 `outcome_unknown`，必须按 Adapter 能力执行查询确认、补偿或人工介入。

## 5. Agent Server 与 Tool Adapter

正式接口、能力模型、远程事件语义和 OpenClaw/Hermes/OpenAI Responses profile 以 [ADAPTER_CONTRACT.md](./ADAPTER_CONTRACT.md) 为准。

### 5.1 通用 Agent Server 契约

Runtime 不依赖第三方 Agent 的内部 Loop。`AgentServerAdapter` 至少抽象以下领域操作：

```rust
trait AgentServerAdapter {
    async fn capabilities(&self) -> Result<AgentCapabilities>;
    async fn submit(&self, request: AgentRunRequest) -> Result<RemoteRunRef>;
    async fn status(&self, remote_run: &RemoteRunRef) -> Result<RemoteRunStatus>;
    async fn events(&self, request: AgentEventRequest) -> Result<AgentEventStream>;
    async fn stop(&self, request: StopAgentRun) -> Result<StopOutcome>;
    async fn result(&self, remote_run: &RemoteRunRef) -> Result<AgentRunResult>;
}
```

- Adapter 必须声明是否支持远程 Run ID、流式事件、状态查询、停止、恢复、幂等提交、会话延续和 Artifact 返回。
- Runtime 只依赖能力声明和规范化事件，不依赖第三方私有数据结构；原始事件可以作为受控审计附件保存。
- Runtime 在外部提交前先持久化 AgentExecution 意图，提交后保存远程 Run/Session 引用；事件消费游标、能力快照、停止请求与停止结果必须持久化，Worker 重启后可继续查询或续读。
- 远程服务不支持幂等提交或按幂等键查询时，提交结果不确定必须标记为 `outcome_unknown`，禁止自动创建第二个远程 Run。
- 首批候选为 OpenClaw Gateway/Server、Hermes Agent API Server 和 Codex 官方服务端/API 能力。具体端点与协议版本在 Adapter 实现时验证并固定，不能从 CLI 输出格式推断稳定协议。
- 不满足远程 Server 模式的 Agent 可以暂不支持；不得用 `spawn` CLI 子进程伪装成生产 Server Adapter。
- Adapter 认证信息使用凭据引用，不写入 Event、Checkpoint 或一般查询响应。

### 5.2 Tool Adapter 与 Agent Server Adapter 的边界

- Agent Server Adapter 执行一个可产生多步推理、工具事件和交付物的远程 Agent Run。
- Tool Adapter 执行单个边界清晰的外部能力，例如代码仓库、测试平台、制品库、审批系统或 DevOps 部署。
- 两类 Adapter 都必须接受 correlation、causation 和 idempotency 信息，并返回统一错误分类；部署等不可逆操作必须支持状态查询或补偿说明。

## 6. PostgreSQL 与 MySQL Provider 要求

### 6.1 共同最低能力

- ACID 事务与行级锁；
- 唯一约束与条件更新；
- `SELECT ... FOR UPDATE SKIP LOCKED`；
- JSON 类型用于 payload 与 checkpoint；
- UTC 时间戳、精确截止时间与分页查询；
- 可执行版本化数据库迁移；
- 连接池、超时、死锁重试和可观测的数据库错误分类。

### 6.2 通用性约束与可选能力

- Runtime crate 不得导入 PostgreSQL/MySQL 驱动类型，不得拼接 SQL，不得依赖某一数据库专属通知或 JSON 查询语义。
- 两个 Provider 共享领域命令、返回类型、迁移语义和黑盒一致性测试；方言差异封装在 Provider 内部。
- 共享逻辑模型不要求迁移脚本逐字相同，但表、约束、索引和事务结果必须语义等价。

Provider 可暴露能力声明，例如：`WakeupNotification`、`FullTextSearch`、`JsonPathQuery`、`ReadReplica`、`PartitionManagement`。Runtime 的正确性不得依赖这些可选能力：通知失效时回退到轮询，搜索能力缺失时仅限制查询功能。

### 6.3 数据保留与扩展

- `events`、`trace_spans` 等追加型大表在容量需要时再分区；事件分区必须通过唯一守卫表或 Provider 等价机制保留 `(tenant_id, run_id, sequence)` 唯一性。
- `runs`、`tasks`、`tool_executions` 保持为高频 OLTP 表，并针对领取/查询路径建立复合索引。
- 事件保留、归档与删除要有独立策略；删除前确保 Run 的审计与合规要求已满足。
- 读副本只服务可容忍延迟的查询；任何状态推进、任务领取和幂等检查必须访问主库。

## 7. Redis 与消息系统定位

Redis 可以后置引入，但只能承担可丢失的加速职责：

| 场景 | 适用性 | 约束 |
| --- | --- | --- |
| API 限流、配额窗口 | 适用 | 容许短暂不精确 |
| Agent Definition / Tool Registry 缓存 | 适用 | 可从数据库重建 |
| SSE / WebSocket 在线广播 | 适用 | 客户端必须能从 Event Store 补拉 |
| Run 只读视图缓存 | 谨慎适用 | 短 TTL 或正确失效 |
| Task 真相、Lease 真相、Checkpoint、幂等结果 | 不适用 | 只能存在 DurableStore |

Kafka、NATS 或 Redis Streams 可以在高吞吐事件分发时引入，但必须遵循 Transactional Outbox：数据库先原子写入业务状态与 outbox 记录，再异步投递。投递失败可重试，消费者必须幂等。

## 8. 分阶段实施

### Phase 0：设计与验证

- 定义 Event、CommandReceipt、Run、Task、Checkpoint、ToolExecution 的稳定领域类型。
- 定义 WorkflowDefinition、StageExecution、WaitSubscription、ArtifactRef、AgentEndpoint、AgentExecution 等领域类型。
- 评审并冻结 [DOMAIN_MODEL.md](./DOMAIN_MODEL.md) 的实体关系、唯一约束、索引意图与 Provider 类型映射。
- 绘制状态机、命令/事件合法转换矩阵和竞争优先级。
- 定义 `DurableStore` 契约、错误分类和 Provider capability 模型。
- 评审并冻结 [STORE_CONTRACT.md](./STORE_CONTRACT.md) 的命令、结果、可靠后续动作与 PostCommitHint 语义。
- 定义 Agent Server/Tool Adapter 契约、能力协商和版本兼容策略。
- 评审并冻结 [ADAPTER_CONTRACT.md](./ADAPTER_CONTRACT.md) 的提交、续读、停止、审批、指导和未知结果恢复语义。
- 评审并冻结 [E2E_SCENARIO.md](./E2E_SCENARIO.md) 的业产研阶段、Artifact Contract、质量门禁与验收路径。
- 评审并冻结 [MIGRATION_DESIGN.md](./MIGRATION_DESIGN.md) 的共享 migration ID、复合约束、锁/CAS SQL 形状和在线演进规则。
- 编写与实现无关的一致性测试规范。

验收：PostgreSQL/MySQL Provider 在接口层可替换；不存在任何 Runtime 代码直接依赖 SQL 方言；状态竞争和 Adapter 能力降级均有可执行规范。

### Phase 1：基础持久化 Runtime

- 实现 PostgreSQL Provider、迁移与开发环境。
- 实现 Run 创建、查询、事件查询、Task 领取、Lease、Worker 基础循环。
- 实现 Workflow Definition、Stage Execution、业产研流程骨架、Event Log、Checkpoint 和基础状态机。
- 提供 Workflow/Run 创建查询及事件查询 API。
- 实现基础 tenant 隔离、认证边界、结构化日志、correlation/causation Trace 和审计查询。
- 实现 Mock Agent Server Adapter，跑通需求分析到部署的模拟垂直链路。

验收：任意 Worker 在 Task 执行中被终止后，Lease 到期可由另一 Worker 恢复；Run 不丢失且事件顺序可审计；模拟业产研链路可完整运行。

### Phase 2A：控制、恢复与副作用

- 支持暂停、恢复、取消、全局截止时间和超时任务。
- 实现 WaitSubscription、人工审批、外部回调、子 Run 等待和定时唤醒。
- 实现 ToolExecution、重试策略、退避与 Dead Letter 状态。
- 接入 SSE；所有实时事件断线后可从 Event Store 续读。
- 实现至少一个真实 Agent Server Adapter 和一个 DevOps Tool Adapter。

验收：重复 Webhook、重复 Task 执行、Lease 失效、取消/暂停竞争和审批事件乱序均不破坏 Run 状态机；完成一次正常交付、一次暂停恢复和一次测试失败返工。

### Phase 2B：MySQL Provider 对等实现

- 实现 MySQL 8+ / InnoDB Provider 与版本化迁移。
- 运行与 PostgreSQL 完全相同的领域一致性、并发、故障注入和 Adapter 集成测试。
- 记录无法语义等价的数据库差异；不得通过 Runtime 分支改变领域行为。

验收：同一业产研场景及全部一致性测试在 PostgreSQL 和 MySQL 上产生等价领域结果。

### Phase 3：编排与平台能力

- 支持子 Run、Fan-out/Fan-in、Handoff 与条件分支。
- 引入 Agent/Tool Registry、多租户配额和预算控制。
- 增加高级 Trace 检索、审计分析、重放与失败恢复工具。
- 基于测量结果引入 Redis 或消息系统的加速实现。

验收：所有扩展仍能通过 DurableStore 契约与一致性测试；优化组件故障不导致数据或执行语义丢失。

## 9. 一致性测试清单

每个生产 DurableStore Provider 必须通过相同测试：

- 并发 Worker 只能有一个成功领取同一 Task；
- 同一 idempotency key 的重复事件只产生一次状态推进；
- Lease 过期后旧 Worker 的完成请求被拒绝；
- Worker 在副作用前后崩溃时，系统按幂等策略恢复；
- Run 取消与 Task 完成并发时，结果符合既定优先级；
- Run 暂停提交后不再领取或生成执行 Task，迟到完成不能推进状态；恢复后从最后成功 checkpoint 继续；
- 审批事件、回调事件重复或乱序时不产生非法状态；
- 同一个 WaitSubscription 只能被一个匹配事件消费，不匹配或过期事件具有稳定错误结果；
- ToolExecution 处于 `outcome_unknown` 时不会盲目重放非幂等副作用；
- AgentExecution 在远程提交前后发生 Worker 崩溃时不会产生不可追踪的重复远程 Run，持久化事件游标可用于续读；
- 事务中途失败时，不存在“事件已写、checkpoint 未写”或“task 完成、后续 task 缺失”的部分提交；
- Provider 的可选通知/缓存能力完全失效时，轮询路径仍能完成执行；
- PostgreSQL 与 MySQL 对同一命令序列、并发场景和故障注入产生等价领域结果；
- Agent Server 不支持停止、恢复或事件流时，Adapter 按能力声明降级，不伪造已停止或已恢复状态。

## 10. 当前不做的事情

- 不把所有 Agent 行为强制建模为固定 DAG；
- 不在 v1 绑定单一 LLM、单一 MCP 或单一消息队列；
- 不把 Redis 作为持久化任务队列或唯一锁服务；
- 不承诺跨外部服务的全局 exactly-once；
- 不在尚无压测和运行数据时引入 Kafka、分库分表或多区域主动写入；
- 不以 `spawn` OpenClaw、Hermes Agent、Codex 或其他 CLI 子进程作为生产集成方案；
- 不在首个版本内建完整原型、代码托管、CI/CD 或制品平台；通过 Adapter 集成现有系统。

## 11. 下一步

已完成共享领域/Store/Adapter 契约骨架、`0000` 至 `0009` PostgreSQL/MySQL 对等迁移、migration runner/执行状态机，以及 PostgreSQL migration executor。PostgreSQL 事务垂直切片现已覆盖 Run 创建/查询、Event 分页、Task 生命周期、Wait 事件应用、ToolExecution 两阶段记录、AgentExecution 提交/事件/结果记录和 Pause/Resume/Cancel；Worker、事件、外部执行与控制命令统一使用显式层级锁序。失败路径已区分 retry、fatal 与 Dead Letter，Wait 持久化恢复计划，Tool/Agent backoff 持久化 due time，Agent event receipt、local Event、cursor 与 Run sequence 同事务提交。后续按以下顺序推进：

1. 在 CI 的 PostgreSQL 16+ 测试库启用 `AGENT_LOOM_TEST_POSTGRES_URL`，持续执行真实 migration、重复命令、领取/续租/完成/失败、暂停/恢复/取消和 cancel/complete 竞争 smoke test；继续补充多 Worker 领取、续租/回收竞争与故障注入测试。
2. 实现 reconcile Task 领取后的 Tool retry attempt / Agent resubmit 启动事务；数据库时间与 `(due_at, kind, execution_id)` keyset 候选扫描、revision 复核与 due-work 应用事务，以及 Agent 规范化事件的确定性投影已经进入 PostgreSQL 路径。
3. 将共享 conformance harness 从 schema 形状测试扩展为 Provider 黑盒行为测试，并实现 MySQL 对等事务路径。
4. 实现 Scheduler/Worker 最小循环、Lease 回收与数据库时间驱动的 due-work 扫描。
5. 实现 Mock Agent Server Adapter 和业产研 Workflow fixture，跑通需求分析至部署的模拟 E2E。
6. 按部署需求选择 `0010_optional_outbox`；将 `0011_runtime_grants` 作为数据库权限加固迁移或等价 IaC，而不是基础领域正确性的前置条件。
