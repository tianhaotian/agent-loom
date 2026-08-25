# Agent Loom 状态机与并发语义规范

## 1. 目的与适用范围

本文是 Agent Loom Phase 0 的状态机基线，约束 Run、StageExecution、Task、WaitSubscription、ToolExecution 和 AgentExecution 的状态、事件、事务边界、幂等行为与竞争裁决。

本文中的“必须”“不得”属于实现与 Provider 一致性测试的强制要求。PostgreSQL 与 MySQL Provider 可以使用不同 SQL，但必须产生相同的领域结果。

## 2. 全局原则

### 2.1 权威状态与时间

- DurableStore 是状态、Lease、Checkpoint、等待条件、事件序列和幂等结果的唯一真相。
- 状态判断、Lease 过期、deadline 和延迟任务到期统一使用数据库权威时间。
- Worker 内存、SSE 连接、消息系统和缓存均不得作为状态推进依据。
- 外部 Agent、Tool 或 DevOps 调用不得持有数据库事务或行锁。

### 2.2 原子状态转换

每次业务状态转换必须在一个数据库事务中完成：

1. 验证 tenant、幂等键和请求摘要；
2. 锁定或 CAS 校验当前领域对象；
3. 验证当前状态、版本、Lease 和 deadline；
4. 追加不可变 Event；
5. 更新状态投影和版本；
6. 写入 Checkpoint、ArtifactRef 或执行结果；
7. 创建后续 Task、WaitSubscription 或恢复动作；
8. 保存本次命令的确定性结果；
9. 提交事务。

任一步骤失败必须整体回滚。通知、SSE 广播和消息投递只能在事务提交后发生，并允许从 Event Store 补发。

### 2.3 竞争裁决

并发命令不使用进程内优先级。对同一个 Run 的竞争采用以下规则：

- 数据库串行化后，第一个成功提交的合法转换获胜。
- 后续事务必须重新读取状态和版本，不能依据事务外的旧快照推进。
- 终态不可被普通命令或事件重新打开。
- 控制命令的“立即生效”是指其事务提交后立即成为唯一权威状态，不表示可以撤销事务提交前已经发生的外部副作用。

### 2.4 锁顺序

需要锁定多个已有对象时，统一采用以下顺序，避免不同 Provider 出现相反锁序：

```text
Tenant（命令依赖 tenant active 状态时）
  → Run
  → StageExecution
  → Task
  → WaitSubscription
  → ToolExecution / AgentExecution
```

- 领取 Task 可以先无锁选择候选 ID，但正式领取事务必须先以共享锁验证 Tenant active，再按上述顺序锁定 Run 和 Task，并重新检查候选条件。
- 多个同类对象按主键升序锁定。
- 控制命令优先只更新 Run 控制门和版本；Task/Wait 的批量收尾可以由同事务中的有序更新或后续幂等清理完成，不得引入反向锁序。

### 2.5 事件顺序

- `runs.next_event_sequence` 保存下一个可分配序号。
- 追加 Event 时必须锁定 Run 或以 Run 版本 CAS 分配序号。
- `(run_id, sequence)` 必须唯一，sequence 在单个 Run 内严格递增。
- Run 状态改变必须产生对应状态 Event；同一状态转换不得产生两个成功 Event。
- 被拒绝的命令可以产生 `command.rejected` 审计事件，但相同幂等请求重复到达时不得重复追加。

### 2.6 命令幂等

系统逻辑上需要 `CommandReceipt`：

```text
tenant_id, scope, idempotency_key, request_hash, outcome,
event_id, resource_version, created_at, expires_at
```

唯一约束为 `(tenant_id, scope, idempotency_key)`。

- 首次请求保存请求摘要与最终结果。
- 相同 key、相同 request hash 返回原结果，不再次执行转换。
- 相同 key、不同 request hash 返回 `IDEMPOTENCY_KEY_REUSED`。
- 已接受、已拒绝和终态 no-op 都应保存确定性结果。
- Receipt 的保留期不得短于对应 Webhook、客户端或 Adapter 的最大重试窗口。

## 3. 统一命令结果

领域命令返回以下结果之一：

| 结果 | 含义 | 是否可安全重试 |
| --- | --- | --- |
| `applied` | 本次请求完成一次新转换 | 不需要；重复请求返回原结果 |
| `duplicate` | 已存在相同幂等请求的结果 | 是 |
| `no_op` | 当前状态已满足请求，例如重复取消 | 是 |
| `rejected` | 请求在当前状态不合法 | 取决于错误分类 |
| `conflict` | 版本、Lease 或并发条件已变化 | 重新读取后决定 |
| `outcome_unknown` | 外部系统可能已执行但无法确认 | 不得盲目重试 |

返回结果至少包含当前状态、当前版本、关联 Event ID，以及稳定的错误代码或原始成功结果。

## 4. Run 状态机

### 4.1 状态定义

| 状态 | 定义 | 核心不变量 |
| --- | --- | --- |
| `queued` | 存在可领取的执行 Task | 至少一个 Task 可在当前时间和控制门下领取 |
| `running` | 至少一个有效 Lease 的 Task 正在执行 | 有效 Task generation 与 Run generation 一致 |
| `waiting` | 没有可执行 Task，等待非审批外部条件 | 至少一个 open WaitSubscription |
| `approval_required` | 没有可执行 Task，等待人工审批 | 至少一个 open approval WaitSubscription |
| `retrying` | 没有立即可执行 Task，存在未来重试 | 至少一个 retry Task 的 `available_at` 在未来 |
| `paused` | 控制面冻结执行 | 不得领取或生成可执行 Task；允许持久化被暂停门阻塞的恢复 Task |
| `completed` | 所有必需阶段和最终产物已完成 | 终态 |
| `failed` | 遇到不可恢复错误或失败策略终止 | 终态 |
| `cancelled` | 用户或系统取消成功提交 | 终态 |
| `timed_out` | 全局 deadline 成功终止 Run | 终态 |

Run 还必须保存：

```text
version, execution_generation, checkpoint_id, suspended_from_status,
deadline, next_event_sequence, terminal_event_id
```

`execution_generation` 用于阻止暂停、取消或恢复之前的迟到 Worker 结果推进新状态。

### 4.2 状态投影规则

除控制命令和终态转换外，Run 状态由其有效子状态投影：

1. 任一当前 generation Task 持有有效 Lease：`running`；
2. 否则存在当前可领取 Task：`queued`；
3. 否则存在 open approval WaitSubscription：`approval_required`；
4. 否则存在其他 open WaitSubscription：`waiting`；
5. 否则存在未来可用的 retry Task：`retrying`；
6. 所有必需 StageExecution 成功且最终产物满足契约：`completed`；
7. 其他情况属于不变量破坏，不得猜测状态，返回 `INCONSISTENT_PROJECTION` 并触发告警。

`paused` 和所有终态覆盖上述投影规则。

### 4.3 转换矩阵

| 当前状态 | 命令或事件 | 条件 | 下一状态 |
| --- | --- | --- | --- |
| 任意非终态 | `pause` | 幂等键有效 | `paused` |
| `paused` | `resume` | 没有未处理的未知副作用 | 按子状态重新投影 |
| 任意非终态 | `cancel` | Run 尚未进入终态 | `cancelled` |
| 任意非终态 | `deadline.expired` | 数据库时间不早于 deadline | `timed_out` |
| `queued` | `task.claimed` | Task 可领取且 generation 匹配 | `running` |
| `running` | `task.completed` | 仍有有效 Lease | `running` |
| `running` | `task.completed` | 有后续可领取 Task | `queued` |
| `running` | `wait.created` | 没有其他有效执行 Task | `waiting` / `approval_required` |
| `running` | `retry.scheduled` | 没有其他有效执行 Task | `retrying` |
| `waiting` / `approval_required` | `wait.matched` | Run 未暂停 | `queued` |
| `retrying` | `retry.due` | Run 未暂停 | `queued` |
| `running` | `run.succeeded` | 所有完成条件满足 | `completed` |
| 任意非终态 | `fatal.error` | 错误不可恢复 | `failed` |
| 任意终态 | 普通命令或事件 | 无系统级恢复授权 | 状态不变 |

### 4.4 Pause

Pause 事务必须：

1. 锁定 Run，校验幂等键、当前状态和版本；
2. 保存 `suspended_from_status`；
3. 将 Run 置为 `paused`；
4. 递增 `version` 和 `execution_generation`，使旧 Task completion 失效；
5. 冻结后续领取和 Task 生成；
6. 追加 `run.paused`，保存 CommandReceipt；
7. 提交后向活跃 AgentExecution/ToolExecution 发出尽力停止请求。

已经发生的远程副作用不能通过 Pause 回滚。无法确认停止结果时，执行记录进入 `outcome_unknown`，Run 保持 paused，并标记 `resume_blocked_reason`。

暂停期间到达的合法 Webhook、审批或子 Run 结果可以被幂等接收和消费，但只能创建被暂停控制门阻塞的恢复 Task，Run 继续保持 `paused`。

### 4.5 Resume

Resume 事务必须：

1. 锁定 Run 并确认状态为 `paused`；
2. 检查所有 `outcome_unknown` 执行是否已对账、补偿或获得人工决策；
3. 重新验证 Checkpoint schema、Workflow/Agent Definition 版本和 ArtifactRef；
4. 检查暂停期间已经消费的等待事件；
5. 基于最新成功 Checkpoint 和当前 `execution_generation` 创建或解冻恢复 Task；
6. 清除暂停控制门，按子状态重新投影 Run；
7. 追加 `run.resumed` 并提交。

Resume 不得简单把 `suspended_from_status` 写回 Run；等待条件、deadline 和远程执行结果可能在暂停期间已经变化。

### 4.6 Cancel、Complete 与 Timeout

- `cancel`、`run.succeeded` 和 `deadline.expired` 通过同一 Run 锁或 CAS 串行化。
- 第一个成功提交的合法终态获胜，后续命令返回 `no_op` 或 `TERMINAL_RUN`。
- Cancel/Timeout 提交后递增 `execution_generation`，关闭 open WaitSubscription，阻止 Task 领取，并在事务后尽力停止远程执行。
- Complete 事务必须同时提交最终 Event、Checkpoint、StageExecution、ArtifactRef 和 `terminal_event_id`。
- 终态 Event 通过 `terminal_event_id` 或等价唯一约束确保只能存在一个。

## 5. Task 状态机

### 5.1 状态

```text
scheduled → queued → leased → succeeded
               │        ├→ retry_scheduled → queued
               │        ├→ failed
               │        └→ dead_lettered
               └────────────────────────────→ cancelled
```

| 状态 | 含义 |
| --- | --- |
| `scheduled` | 尚未到 `available_at` |
| `queued` | 当前可被领取 |
| `leased` | 被一个 Worker 持有有效 Lease |
| `retry_scheduled` | 失败后等待下一次尝试 |
| `succeeded` | 结果已原子提交 |
| `failed` | 不可重试失败 |
| `dead_lettered` | 达到最大尝试次数，等待恢复策略或人工介入 |
| `cancelled` | 因 Run 终止、Stage 终止或 generation 失效而关闭 |

### 5.2 Task 不变量

- Task 必须保存 `generation`、`based_on_checkpoint_sequence` 和 `attempt`。
- 领取条件必须包含 Run 非 paused/terminal、Task generation 等于 Run generation、`available_at <= database_now`。
- 完成和续租必须匹配 `task_id + lease_owner + lease_token + lease_expires_at > database_now`。
- 一次成功完成只能产生一个 Task completion Event 和一组后续动作。
- Pause 后旧 generation Task 的迟到结果只作为执行证据记录，不得更新 Run、StageExecution 或 Checkpoint。
- Dead Letter 不是 Run 终态；Workflow 策略决定转人工、返工或将 Run 置为 failed。

### 5.3 Lease 回收

- Scheduler 只能回收数据库时间已经过期的 Lease。
- 回收事务以 Task 当前 lease token 为条件，递增 attempt，并转为 `retry_scheduled`、`queued`、`dead_lettered` 或 `cancelled`。
- 旧 Worker 随后提交时返回 `LEASE_LOST`，不能覆盖新 Worker 结果。
- 续租和回收竞争时，第一个成功的条件更新获胜。

## 6. StageExecution 状态机

```text
planned → active → waiting_approval → succeeded
             │              │
             ├→ rework_required → active
             ├→ failed
             ├→ skipped
             └→ cancelled
```

| 当前状态 | 触发 | 下一状态 | 约束 |
| --- | --- | --- | --- |
| `planned` | 阶段输入满足 | `active` | 创建首批 Task |
| `active` | 需要人工门禁 | `waiting_approval` | 创建 approval WaitSubscription |
| `waiting_approval` | approve | `active` 或 `succeeded` | 由门禁契约决定是否仍有工作 |
| `waiting_approval` | reject | `rework_required` / `failed` | 由 Workflow 策略决定 |
| `active` | 质量检查失败且可返工 | `rework_required` | 保存失败报告 ArtifactRef |
| `rework_required` | 返工计划生成 | `active` | attempt 递增并关联原 StageExecution |
| `active` | Artifact Contract 全部满足 | `succeeded` | 产物与状态同事务提交 |
| 非终态 | Run cancel/timeout | `cancelled` | 不得继续生成 Task |

StageExecution 的业务状态与 Task 技术执行状态分离。一个 Stage 可以包含多个 Task；Task 成功不等于 Stage 成功，必须满足阶段 Artifact Contract 和质量门禁。

下游门禁失败或需求变更要求重做已成功阶段时，不把历史 `succeeded` StageExecution 改回非终态；系统创建相同 `stage_key`、递增 attempt 且关联 parent/causation 的新 StageExecution。Run 完成判定使用每个必需 stage_key 的最新有效 attempt，并验证其输入 Artifact version 未被后续变更失效。

## 7. WaitSubscription 状态机

```text
open → consumed
  ├──→ expired
  └──→ cancelled
```

匹配与消费事务必须：

1. 锁定 Run，再锁定 WaitSubscription；
2. 验证 tenant、event type、match key、payload contract 和签名结果；
3. 检查状态为 `open` 且数据库时间早于 `expires_at`；
4. 保存外部事件 CommandReceipt；
5. 将 WaitSubscription 置为 `consumed` 并保存 `consumed_by_event_id`；
6. 追加领域 Event；
7. 创建恢复 Task，或在 Run paused 时创建受控制门阻塞的恢复 Task；
8. 更新 Checkpoint 和 Run 投影；
9. 提交事务。

两个事件竞争同一个 WaitSubscription 时只能有一个进入 `consumed`。失败者返回 `WAIT_ALREADY_CONSUMED`，相同幂等请求返回首次结果。

Timer 过期与外部事件竞争时同样采用首个合法提交获胜：Timer 先提交则外部事件返回 `WAIT_EXPIRED`；外部事件先消费则 Timer no-op。

## 8. ToolExecution 状态机

```text
planned → executing → succeeded
                    ├→ retry_scheduled → executing
                    ├→ failed
                    └→ outcome_unknown → reconciling
                                          ├→ succeeded
                                          ├→ failed
                                          ├→ compensated
                                          └→ manual_review
```

- `tool_call_id` 在逻辑工具调用生命周期内稳定，attempt 单独递增。
- 对外请求始终传递相同 idempotency key；不得因 Worker 重启生成新 key。
- 外部调用前先持久化 `planned/executing` 意图，调用后再以短事务保存结果。
- 进程在外部响应与本地提交之间崩溃时，执行进入或被恢复器判定为 `outcome_unknown`。
- `outcome_unknown` 只能通过外部查询、幂等重放、补偿或人工决定退出，不能由普通 Task 自动重试。
- 已 `succeeded` 的调用在 Task 重试时直接复用结果。

## 9. AgentExecution 状态机

```text
planned → submitting → running → succeeded
                 │         ├→ stopping → cancelled / succeeded / outcome_unknown
                 │         └→ failed
                 └──────────→ outcome_unknown → reconciling → running / failed / manual_review
```

- 提交前持久化 `execution_id`、Endpoint、能力快照、请求摘要和幂等键。
- 提交成功后持久化 `remote_run_ref`；没有该引用时不得假设远程提交失败。
- 远程 Agent 进入 running 后，提交 Task 必须保存 Checkpoint，创建 callback WaitSubscription 或短时 poll Task 并释放 Worker；不得让 Worker 通过长连接或循环轮询等待整个远程 Run 完成。
- Adapter 支持按幂等键查询时，恢复器优先查询既有远程 Run。
- Adapter 不支持幂等提交或查询确认时，提交窗口故障进入 `outcome_unknown`，禁止自动创建第二个远程 Run。
- 事件游标必须持久化；每批远程事件的规范化、游标更新和本地 Event 追加应原子提交。
- `stop` 是请求，不等于远程已经停止。只有 Adapter 明确确认后才能进入 `cancelled`；远程已经完成时应记录真实结果，再由 Run generation 决定该结果能否推进业务状态。
- Pause/Cancel 后的迟到远程结果可以保存为审计与对账数据，但不能绕过 Run 版本和 generation 校验。

## 10. 关键事务

### 10.1 Complete Task

```text
BEGIN
  validate command receipt
  lock Run
  lock StageExecution（如存在）
  lock Task
  validate Run state/version/generation/deadline
  validate Task lease owner/token/expiry
  validate execution result and artifact contracts
  allocate Event sequence
  update Task
  update StageExecution / ArtifactRef
  write Checkpoint
  create next Task or WaitSubscription
  project Run state and increment version
  append Event
  save CommandReceipt
COMMIT
```

### 10.2 Apply External Event

```text
BEGIN
  validate command receipt and signature result
  lock Run
  lock matching WaitSubscription
  validate event contract and expiry
  consume WaitSubscription
  append Event
  update Checkpoint / StageExecution
  create recovery Task, respecting pause gate
  project Run state unless paused
  save CommandReceipt
COMMIT
```

### 10.3 Control Command

```text
BEGIN
  validate command receipt
  lock Run
  validate expected version and transition
  set pause/cancel/resume state and execution_generation
  close or freeze child work according to command
  append Event
  save CommandReceipt
COMMIT
perform best-effort remote stop after commit
```

## 11. 竞争场景裁决表

| 竞争 | 获胜条件 | 失败方结果 |
| --- | --- | --- |
| `complete` vs `cancel` | 首个合法终态提交 | 读取终态并返回 no-op/conflict |
| `complete` vs `pause` | 首个合法提交 | Pause 先提交则旧 completion 因 generation/version 失效；Complete 先提交则 Pause 作用于新投影或终态 no-op |
| `complete` vs `deadline` | 首个合法终态提交 | 失败方不得覆盖 terminal Event |
| `renew_lease` vs `reclaim_lease` | 首个满足 token/expiry 条件的更新 | 失败方返回 `LEASE_LOST` |
| 两个 Worker 完成同一 Task | 首个有效 Lease completion | 另一个返回 duplicate 或 `LEASE_LOST` |
| 两个事件消费同一 Wait | 首个消费提交 | 另一个返回 `WAIT_ALREADY_CONSUMED` |
| `wait.matched` vs `wait.expired` | 首个合法提交 | 另一个返回 consumed/expired |
| `retry.due` vs `pause` | Run 锁上的首个提交 | Pause 获胜则 Task 保持受控制门阻塞；retry 获胜后 Pause 仍可使 generation 失效 |
| 远程 `stop` vs remote completion | 记录远程真实结果 | Run 是否接受结果由 version/generation 决定 |
| 重复相同命令 | 首次 CommandReceipt | 返回原结果，不产生新 Event |
| 相同 key、不同 payload | 已存在的 CommandReceipt | `IDEMPOTENCY_KEY_REUSED` |

## 12. 错误分类

| 错误码 | 含义 | 默认重试策略 |
| --- | --- | --- |
| `INVALID_TRANSITION` | 当前状态不允许该转换 | 不自动重试 |
| `VERSION_CONFLICT` | Run 版本已经变化 | 重新读取后判断 |
| `TERMINAL_RUN` | Run 已进入终态 | 不重试 |
| `LEASE_LOST` | Lease token/owner 已失效 | 当前 Worker 停止提交 |
| `LEASE_EXPIRED` | 数据库时间已超过 Lease | 当前 Worker 停止提交 |
| `IDEMPOTENCY_KEY_REUSED` | 同一 key 对应不同请求 | 不重试，修复调用方 |
| `WAIT_MISMATCH` | 事件与等待契约不匹配 | 不自动重试 |
| `WAIT_ALREADY_CONSUMED` | 等待已由其他事件消费 | 视为确定结果 |
| `WAIT_EXPIRED` | 等待已经超时 | 不自动重试 |
| `DEADLINE_EXCEEDED` | Run/Task/Tool deadline 已到 | 不自动重试 |
| `PAUSE_RECOVERY_REQUIRED` | 存在未知副作用，暂不能恢复 | 对账或人工处理 |
| `OUTCOME_UNKNOWN` | 外部执行结果无法确认 | 禁止盲目重试 |
| `ADAPTER_CAPABILITY_MISSING` | Adapter 不支持所需语义 | 降级或更换 Adapter |
| `INCONSISTENT_PROJECTION` | 子状态无法投影出合法 Run 状态 | 告警并停止推进 |
| `TENANT_MISMATCH` | 资源不属于当前 tenant | 拒绝并安全审计 |

错误日志必须包含 tenant、run、stage、task、event、correlation、causation、producer、当前版本和错误分类；凭据和敏感 payload 不得写入日志。

## 13. 一致性测试与验收

所有生产 DurableStore Provider 必须运行同一套黑盒测试：

1. 100 个并发 Worker 竞争同一 Task，只有一个获得有效 Lease。
2. 旧 Lease Worker 在回收后提交，返回 `LEASE_LOST` 且不产生 Event/Checkpoint。
3. Cancel、Complete 和 deadline 三方并发，最终只有一个 terminal Event。
4. Pause 与 completion 并发，Pause 获胜时迟到结果不能推进 Run；Complete 获胜时 Pause 基于新版本处理。
5. Pause 期间接收 Webhook/审批，事件只消费一次且 Run 保持 paused；Resume 后恢复 Task 只创建一次。
6. 相同幂等键重复 100 次，只产生一次业务转换；同 key 不同 payload 被拒绝。
7. 两个不同事件同时匹配一个 WaitSubscription，只有一个成功消费。
8. Worker 在外部 Tool 调用前、调用中、响应后和本地提交前分别崩溃，结果符合幂等或 `outcome_unknown` 策略。
9. Worker 在远程 Agent 提交前后崩溃，不产生不可追踪的重复远程 Run。
10. Agent SSE/事件流断开后从持久化 cursor 续读，不重复推进本地状态。
11. Stage Task 全部成功但 Artifact Contract 不满足时，Stage 不得进入 succeeded。
12. PostgreSQL 与 MySQL 对相同命令历史和故障注入产生等价 Run、Event、Checkpoint 和错误结果。

建议增加状态机属性测试：随机生成合法与非法命令序列，并持续验证终态不可逆、事件序列唯一、版本单调、单 Wait 单消费、单 Task 单完成和单 Run 单终态 Event。

## 14. 与后续设计的接口

本规范冻结领域语义，不冻结具体表结构或 Rust 类型名称。后续产物必须以本文为依据：

- `DOMAIN_MODEL.md`：落实字段、关系、索引与唯一约束；
- [STORE_CONTRACT.md](./STORE_CONTRACT.md)：把事务转换表达为 Rust trait、命令、错误类型和可靠后置动作；
- [ADAPTER_CONTRACT.md](./ADAPTER_CONTRACT.md)：细化 ToolExecution/AgentExecution 的能力协商与恢复协议；
- [E2E_SCENARIO.md](./E2E_SCENARIO.md)：把业产研交付路径映射到 Stage、Task、Wait、Artifact 与 Event。
- [MIGRATION_DESIGN.md](./MIGRATION_DESIGN.md)：把锁顺序、CAS、唯一约束与终态不变量映射到 PostgreSQL/MySQL 对等 DDL 和事务模板。
