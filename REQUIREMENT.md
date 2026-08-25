# Agent Loom 产品与系统需求

## 1. 背景

传统 Agent 通常以“请求启动一个进程，进程内运行 Agent Loop，返回结果后结束”的方式工作。这种模式适合短同步任务，但无法可靠覆盖服务端 Agent 的典型需求：耗时研究、多步工具调用、外部异步回调、人工审批、定时唤醒，以及服务重启或 Worker 故障后的继续执行。

Agent Loom（织流）旨在提供一个事件驱动、可持久化的 Agent Runtime。系统将 Agent 的推理、工具调用、任务调度、外部事件和人工干预统一表达为状态变化与可审计事件，使一个长期 Agent 任务不依赖任何单一进程的内存或生命周期。

首个产品场景是“业产研交付”全流程编排：从业务需求分析与澄清开始，经过原型与 PRD、技术方案、编码、自测、集成测试，最终通过 DevOps 能力完成部署。流程中的产品、设计、研发、测试、运维人员与多个专业 Agent 可以共同参与，同一交付任务允许按上下文动态生成后续工作，而不是预先固化为不可变 DAG。

## 2. 产品定位

Agent Loom 位于 LLM/Agent SDK、任务队列、持久化工作流和可观测平台之间：

```text
LLM / Agent SDK
        +
Event Bus / Task Queue
        +
Durable Workflow
        +
State / Checkpoint / Audit
        =
Agent Loom Runtime
```

它不是单纯的 Agent SDK，也不是通用 BPM 或消息队列。其核心职责是为服务端 Agent 提供可靠的长期执行内核。

### 2.1 目标用户与首个场景

- 目标用户：建设内部 Agent 平台、研发效能平台或复杂自动化平台的团队。
- 首个场景：端到端业产研交付，覆盖需求分析、原型/PRD、技术设计、编码、自测、集成测试和 DevOps 部署。
- 协作主体：人、平台内置 Agent、外部 Agent Server、业务系统和 DevOps 系统。
- 核心产物：结构化需求、原型或设计引用、PRD、技术方案、代码变更、测试报告、部署记录和审计轨迹。
- 价值判断：流程能够跨小时或跨天可靠运行，允许暂停、审批、返工和动态拆解，并在服务故障后从最后一次成功提交的上下文继续。

## 3. 目标与非目标

### 3.1 目标

- 将每次 Agent 执行建模为可查询、可控制、可恢复的 `Run`。
- 以不可变事件驱动 Run 的创建、执行、等待、恢复和终止。
- 支持长时间运行的模型推理、工具调用和多阶段任务。
- 支持外部 Webhook、定时器、人工审批和子 Agent 结果驱动恢复。
- 支持任务 Lease、重试、超时、取消、幂等和失败恢复。
- 支持多实例无状态部署，并在实例失效后重新调度工作。
- 为开发者提供统一 API、事件模型、审计记录和可观测接口。
- 保持 Agent Definition、模型 Provider、工具 Provider、消息系统与持久化实现的解耦。
- 支持多人、多 Agent 围绕同一交付目标进行分阶段协作、产物交接、质量门禁和动态任务规划。
- 通过远程 Server Adapter 接入不同 Agent Runtime，不要求 Runtime 以子进程方式启动或托管第三方 Agent。

### 3.2 非目标

- 训练或托管基础模型。
- 替代模型供应商 API、MCP Server 或通用企业 BPM。
- 在初期管理全部集群、容器或计算资源。
- 要求用户将 Agent 逻辑强制改写为固定 DAG。
- 承诺跨数据库、LLM、HTTP 工具等外部系统的全局 exactly-once 事务。
- 在 Runtime 领域语义和公共接口中绑定某一种消息队列、缓存或数据库产品；Provider 可以按阶段分先后实现。

## 4. 核心概念

### Agent Definition

Agent 的静态定义。至少包含 Agent ID、系统指令、模型配置、工具集合、能力声明、Handoff 配置、Guardrail、并发限制与预算限制。

### Workflow Definition

复杂流程的版本化定义，描述阶段、角色、进入/退出条件、质量门禁、产物契约、超时与默认策略。Workflow Definition 可以提供初始流程骨架，但运行时允许 Agent 根据上下文生成动态 Task、返工分支和 Handoff，不要求所有行为预先固化为 DAG。

### Stage Execution

Workflow 中某个业务阶段的一次执行实例，用于承载需求分析、PRD、技术方案、编码、测试或部署等阶段的负责人、Agent、输入产物、输出产物、门禁、尝试次数和返工关系。Stage Execution 是业务可查询的进度模型；Task 是其下可独立领取的有限执行单元，两者不得混为同一层级。

### Agent Run

一次具体的长期 Workflow 或 Agent 执行实例，是生命周期和控制操作的载体。顶层 Run 可以关联 Workflow Definition 与协调 Agent；专业 Agent 调用表现为 Task，复杂隔离场景可进一步创建子 Run。至少包含：

```text
run_id, agent_id, tenant_id, workflow_id, status, suspended_from_status,
input, current_state, checkpoint_id, version, created_at, updated_at, deadline
```

### Agent Event

驱动或记录 Run 状态变化的不可变事实。至少包含：

```text
event_id, event_type, run_id, payload, occurred_at, producer,
correlation_id, causation_id, idempotency_key, sequence
```

事件示例：`run.created`、`turn.started`、`model.completed`、`tool.requested`、`tool.completed`、`approval.required`、`approval.received`、`external.callback.received`、`timer.expired`、`run.cancelled`、`run.completed`。

### Command Receipt

外部命令和事件的幂等处理凭据，保存 tenant、作用域、幂等键、请求摘要和首次确定结果。相同 key 与相同请求必须返回原结果；相同 key 对应不同请求必须拒绝。成功、拒绝和终态 no-op 都必须能够稳定重放处理结果。

### Agent Task

可以独立调度、领取、重试和恢复的有限工作单元。任务类型包括模型推理、工具调用、文件处理、数据检索、子 Run、交付物校验和定时唤醒。等待回调或人工审批本身由持久化等待条件表达，不允许通过占用 Worker 实现。

### Checkpoint

在安全恢复点保存的执行快照，包括对话/工作流状态、已完成或待处理的工具调用、等待条件、重试元数据、预算和超时信息。

Checkpoint 必须标识其 schema 版本、Workflow Definition 版本、Agent Definition 版本，以及恢复所需的最近成功用户消息、已确认产物和动态 Task 计划。Runtime 或 Adapter 升级后必须执行兼容性检查或显式迁移，不得静默误读旧 checkpoint。

### Wait Subscription

对外部恢复条件的一等持久化模型。至少包含：

```text
wait_id, run_id, wait_type, match_key, expected_event_type, status,
expires_at, consumed_by_event_id, idempotency_key, created_at
```

`wait_type` 至少支持 approval、webhook、timer、child_run 和 external_signal。一个等待条件只能被一个匹配事件原子消费；重复、过期或不匹配事件不得推进 Run，但必须产生结构化错误或审计记录。

### Artifact Reference

流程阶段产物的版本化引用。Runtime 保存产物元数据、内容摘要、生产者、关联 Task/Run 和外部存储引用；大文件、代码仓库、设计稿和构建产物可保存在专用系统中，不要求全部写入 Event 或 Checkpoint。

### Agent Server Adapter

第三方 Agent 的远程服务适配器。Adapter 将 Runtime 的提交、状态查询、事件流、停止、恢复和结果获取语义映射到具体 Agent Server 协议，并通过能力声明暴露实际支持范围。首批候选包括 OpenClaw、Hermes Agent 和 Codex；只支持 Server、Gateway 或远程 API 模式，不将 spawn 本地子进程作为生产集成方式。

每次远程 Agent 提交必须创建持久化 `AgentExecution`，记录本地 execution ID、Agent Endpoint、稳定幂等键、远程 Run/Session 引用、状态、事件游标、停止结果与最后同步时间。远程提交成功但本地尚未保存引用即崩溃时，Adapter 必须支持按幂等键查询确认，或将结果标记为 `outcome_unknown` 进入人工/对账恢复，不能直接重复提交。

通用 Agent/Tool trait、能力协商、事件规范化、错误映射与官方 Adapter profile 以 [ADAPTER_CONTRACT.md](./ADAPTER_CONTRACT.md) 为准。

## 5. 功能需求

### 5.1 Run 生命周期

系统必须支持创建、查询、暂停、恢复、取消、超时和终止 Run。

```text
queued --task.claimed--> running --next_task.created--> queued
                            ├--wait.created----------> waiting
                            ├--approval.required-----> approval_required
                            ├--retry.scheduled-------> retrying
                            └--run.succeeded---------> completed

waiting / approval_required --matching_event--> queued
retrying --retry.due---------------------------> queued
任意非终态 --pause----------------------------> paused
paused --resume/revalidate---------------------> queued / waiting / approval_required / retrying
任意非终态 --cancel / deadline / fatal--------> cancelled / timed_out / failed
```

核心转换矩阵：

| 当前状态 | 命令或事件 | 下一状态/结果 | 原子副作用 |
| --- | --- | --- | --- |
| `queued` | `task.claimed` | `running` | 领取 Lease，递增 Run 版本 |
| `running` | `task.completed` 且有后续 Task | `queued` | Event、Checkpoint、当前 Task 和后续 Task 同事务提交 |
| `running` | `wait.created` | `waiting` / `approval_required` | 保存 Checkpoint、创建 Wait Subscription、释放 Worker |
| `running` | `retry.scheduled` | `retrying` | 保存错误分类、尝试次数与 `available_at` |
| `waiting` / `approval_required` | 匹配且未消费的事件 | `queued` | 消费等待条件、追加 Event、创建恢复 Task |
| `retrying` | `retry.due` | `queued` | 原子激活重试 Task |
| 任意非终态 | `pause` | `paused` | 保存 `suspended_from_status`、冻结待执行 Task、递增版本 |
| `paused` | `resume` | 经重校验的原状态或 `queued` | 恢复有效等待，或从成功 Checkpoint 创建恢复 Task |
| 任意非终态 | `cancel` | `cancelled` | 终止待执行 Task、关闭等待条件、尽力停止远程执行 |
| 任意非终态 | `deadline.expired` | `timed_out` | 终止待执行 Task、关闭等待条件、记录超时原因 |
| `running` | 最终成功 | `completed` | 最终 Event、Checkpoint 和产物引用同事务提交 |
| 任意非终态 | 不可恢复错误 | `failed` | 记录错误分类和最终可恢复信息 |
| 任意终态 | 普通命令或事件 | 状态不变 | 返回终态/冲突结果，不生成推进事件 |

- 终态 Run 不得被普通事件重新推进。
- 每次状态改变必须生成可审计事件。
- 状态推进必须经过显式状态机校验。
- 每个 Run 必须具有版本或等价并发控制字段。
- 状态变更必须在数据库事务中通过行锁或 CAS/条件更新串行化；更新条件至少包含当前状态与版本号。
- `cancel` 与 `completed` 竞争时采用“首个成功提交的合法终态转换获胜”：若完成先提交，后续取消返回终态不变；若取消先提交，迟到的完成不得覆盖 `cancelled`。终态转换必须产生唯一事件。
- `paused` 表示控制面立即暂停：暂停事务提交后不得领取或创建可执行 Task；允许持久化被暂停门阻塞的恢复 Task。已在执行的 Adapter 收到尽力停止请求，其迟到结果不得推进 Run。系统保留暂停前最后一个成功 checkpoint、最近成功用户消息和尚未执行的动态 Task 计划，恢复后从该提交点继续。
- 非法、不匹配、过期或乱序输入必须返回稳定的错误分类并写入结构化日志；根据安全策略可追加 rejected/ignored 审计事件，但不得改变业务状态。
- 相同幂等键和等价输入必须返回一致的处理结果，且不得重复生成状态事件、后续 Task 或外部副作用。

完整状态定义、转换矩阵、锁顺序、竞争裁决与错误分类以 [STATE_MACHINE.md](./STATE_MACHINE.md) 为准。

### 5.2 事件与外部输入

- 支持 API、Webhook、Cron/Timer 和内部 Worker 产生事件。
- 支持向指定 Run 注入合法外部事件。
- 每个外部事件必须可携带幂等键；重复输入不得重复推进状态或产生重复副作用。
- 事件必须可按 Run 和序列稳定分页查询。
- 外部事件只能触发 Runtime 状态机，不得绕过状态机直接修改 Run。
- 每个 Run 的事件序列必须单调递增，并以 `(run_id, sequence)` 唯一约束保证稳定顺序；幂等去重范围必须在接口契约中明确为 tenant、run 与 producer 的组合。

### 5.3 调度与 Worker

- Task 必须可由多个无状态 Worker 竞争领取。
- 同一个 Task 在有效 Lease 内只能由一个 Worker 完成。
- Worker 应执行有限工作后提交结果，不得在内存中等待长期外部条件。
- Lease 必须可过期、续租和回收；Worker 崩溃后任务必须可被再次领取。
- 支持固定间隔与指数退避、最大尝试次数、可重试错误分类和失败终止。
- 支持 Run、Task 和 Tool Call 的执行/等待超时与全局 deadline。

### 5.4 等待、恢复与人工介入

当 Agent 需要等待审批、Webhook、Timer、子 Run 或其他外部条件时，系统必须：

1. 持久化最新 checkpoint 与等待条件；
2. 将 Run 迁移至适当等待状态；
3. 释放当前 Worker；
4. 创建等待订阅或延迟 Task；
5. 在匹配事件到达时恢复 Run。

审批必须支持批准、拒绝、超时和重复提交去重。

等待匹配、消费 Wait Subscription、写入 Event、更新 Run/Checkpoint 和创建恢复 Task 必须在一个事务中完成。

### 5.5 工具调用与副作用

- 每次工具调用必须拥有稳定的 `tool_call_id` 和 `idempotency_key`。
- 工具执行的请求、结果、错误和尝试次数必须持久化。
- 发生重试时，系统必须优先复用既有成功结果。
- 外部 API、MCP 和代码执行均通过 Tool Adapter 接入；Adapter 不得绕过审计与幂等记录。
- 需要隔离的 CPU 密集、不可信或长耗时工作应以独立 Worker/容器 Task 运行，而非阻塞 Runtime Worker。
- ToolExecution 至少区分 `planned`、`executing`、`succeeded`、`failed` 和 `outcome_unknown`。外部调用成功但本地结果未提交即崩溃时，必须进入可查询确认、补偿或人工介入的恢复路径。
- AgentExecution 与 ToolExecution 使用独立记录；远程 Agent 的 Run ID、事件续读游标、能力快照和停止状态不得只保存在 Worker 内存。

### 5.6 查询与实时输出

最小 API 需支持：

```text
POST /v1/runs
POST /v1/workflows
GET  /v1/workflows/{workflow_id}
GET  /v1/runs/{run_id}
GET  /v1/runs/{run_id}/events
GET  /v1/runs/{run_id}/stages
GET  /v1/runs/{run_id}/artifacts
POST /v1/runs/{run_id}/events
POST /v1/runs/{run_id}/pause
POST /v1/runs/{run_id}/resume
POST /v1/runs/{run_id}/cancel
```

- 支持 SSE 作为首个实时输出通道；WebSocket 和长轮询可以后续补充。
- 断线客户端必须能根据事件序列号从权威 Event Store 补读，不得依赖实时连接不丢消息。
- 创建 Run 时可以引用版本化 Workflow Definition；业产研场景必须能够查询当前阶段、负责人/Agent、待处理门禁和已产出 Artifact Reference。

### 5.7 业产研交付流程

首个端到端流程至少覆盖：

```text
需求输入与澄清
  → 原型设计与 PRD
  → 技术方案与任务规划
  → 编码执行
  → 自测
  → 集成测试
  → DevOps 部署
  → 交付完成
```

- 每个阶段必须声明输入、产物、完成条件、执行主体和可选审批门禁。
- 阶段可以根据评审结果返工，也可以由 Agent 动态拆分为并行或串行 Task。
- 人工反馈作为事件进入状态机，并与原始需求、当前产物和 causation chain 关联。
- 部署属于受控外部副作用，必须记录环境、版本、审批、幂等键、结果与回滚/补偿信息。
- 首个版本不要求内建原型工具、代码托管和 CI/CD 平台，允许通过 Tool Adapter 或 Agent Server Adapter 对接。

## 6. 服务与分布式部署要求

### 6.1 无状态原则

API、Scheduler 和 Worker 服务必须可任意增加、缩减、重启、迁移或替换。任何一个实例消失均不得导致 Run 状态、等待条件、任务、工具结果或幂等记录丢失。

以下内容不得作为唯一事实存于服务内存：

- Run 当前状态和 checkpoint；
- 待处理任务与重试计时；
- Lease 有效性；
- 外部事件处理结果；
- 工具调用的幂等执行结果。

### 6.2 组件职责

| 服务 | 职责 | 禁止承担 |
| --- | --- | --- |
| API Gateway | 请求校验、鉴权、命令与 Webhook 接入、查询、SSE | 执行长期 Agent Loop |
| Scheduler | 扫描超时、延迟任务和过期 Lease，生成恢复任务 | 保存唯一业务状态 |
| Worker | 领取并执行一个 Turn/Task，提交结果 | 长期等待、持有跨请求状态 |
| DurableStore | 保存权威状态、提供原子状态转换 | 执行模型或第三方工具 |

### 6.3 可靠性与一致性

- 默认交付语义为 at-least-once。
- Event、Run 投影、Checkpoint 和后续 Task 的状态变化必须由单一持久化事务原子提交。
- Worker 的外部调用不应持有数据库事务或行锁。
- 旧 Lease 持有者在 Lease 过期后不得覆盖新 Worker 的执行结果。
- 所有控制命令与 Task 完成竞争时必须遵循 [STATE_MACHINE.md](./STATE_MACHINE.md) 定义的数据库串行化和首个合法提交规则。
- 系统必须记录 correlation ID、causation ID、tenant ID 和 actor/producer 信息以支持审计追踪。

## 7. 存储需求与插件化

### 7.1 权威存储要求

持久化层必须同时支持：事务、行级并发控制、唯一约束、条件更新、可靠时间语义、JSON payload/checkpoint 与可分页事件查询。

数据至少逻辑分为：

```text
workflow_definitions / agent_definitions  版本化流程与 Agent 配置
runs              当前状态投影
events            不可变事件与审计日志
tasks             可领取工作、重试与 Lease
task_attempts      Task 领取、Lease 与尝试审计
checkpoints       可恢复执行快照
stage_executions  业务阶段、门禁、负责人和返工关系
tool_executions   外部副作用幂等记录
tool_execution_attempts 外部工具尝试审计
agent_endpoints   远程 Agent Server 配置与能力
agent_executions  远程 Agent Run、事件游标、停止与恢复记录
wait_subscriptions 外部事件、审批、Timer 与子 Run 等待条件
artifact_refs     交付物元数据、版本与外部引用
command_receipts  命令/事件幂等请求摘要与确定结果
outbox            可选：后续异步分发的可靠投递记录
```

字段、关系、索引和跨 Provider 类型映射以 [DOMAIN_MODEL.md](./DOMAIN_MODEL.md) 为准。

### 7.2 DurableStore Provider

存储可插拔，但 Provider 必须实现领域级原子操作，不是泛化 KV/CRUD 接口。最小能力包括：

正式命令、结果、错误、事务边界和提交后动作以 [STORE_CONTRACT.md](./STORE_CONTRACT.md) 为准。

- 原子创建 Run、事件与首个 Task；
- 幂等地应用外部或内部事件；
- 原子领取、续租、完成或失败 Task；
- 原子写入 checkpoint、更新 Run 投影并生成后续 Task；
- 原子推进 Stage Execution、登记阶段产物并生成质量门禁或后续阶段；
- 原子暂停、恢复、取消与审批状态转换；
- 原子创建、匹配、消费和超时 Wait Subscription；
- 原子登记 Artifact Reference 并关联 Run、Task、Event 与版本；
- 持久化 AgentExecution 的提交意图、远程引用、事件游标、状态和幂等结果；
- 原子保存并查询 Command Receipt，区分重复请求与幂等键冲突；
- 查询 Run、Event、Task 和审计记录；
- 支持迁移、错误分类和一致性测试。

初期支持等级：

| Provider | 生产支持 | 说明 |
| --- | --- | --- |
| PostgreSQL | 是，首个实现 | 参考实现 |
| MySQL 8+ / InnoDB | 是 | 必须满足同一事务与并发测试 |
| SQLite | 仅开发/测试 | 不作为高可用多 Worker 生产方案 |
| Redis | 否 | 不具备本系统所需的权威审计与事务语义 |

### 7.3 缓存与消息系统

Redis 可用于限流、配置缓存、热点只读缓存和 SSE/WebSocket 在线广播，但缓存失效不能影响任务执行的正确性。Run、Task、Lease、Checkpoint、Event 和 Tool Execution 的唯一真相必须在 DurableStore。

消息系统可后续用于高吞吐分发；引入后必须使用 Transactional Outbox，并保证消费者幂等。消息系统不可替代 Event Store 的审计与恢复职责。

## 8. 非功能需求

### 可观测性与审计

- 每个请求、事件、Task、模型调用和工具调用应可通过 correlation/causation ID 串联。
- 提供结构化日志、指标和分布式 Trace 接口。
- 记录状态变更原因、生产者、重试次数、错误分类和关键时间点。
- 支持按 tenant、agent、run、event、tool_call 查询审计轨迹。

### 安全与多租户

- 所有 API、Webhook 和实时订阅需要认证与授权边界。
- Run、Event、Checkpoint、工具凭据和审计信息必须具备 tenant 隔离。
- Webhook 必须支持签名校验、重放防护和幂等键。
- 工具凭据不得写入 Event payload 或可被一般查询接口读取的 checkpoint。

### 性能与容量

- 高频 Task 领取与状态提交应为短事务。
- 追加型事件表需预留分区、保留和归档能力；启用时间分区时不得破坏 Run 内事件序列唯一性，初期可先使用未分区表。
- 查询与控制面可独立扩展，读副本仅用于可容忍延迟的只读查询。
- 性能目标应在 PoC 后依据真实 Run 时长、事件量、Task 吞吐和租户并发设定，而非预设不具依据的 QPS。

## 9. 分期范围

### 第一阶段：基础运行时

- Run、Event、Task 与基础状态机；
- PostgreSQL Provider；
- Worker、Lease、基础重试；
- Workflow Definition、Stage Execution、基础 Checkpoint、业产研流程骨架和 Run 创建/查询 API；
- 基础 tenant 隔离、结构化日志、correlation/causation Trace 与审计查询；
- Agent Server Adapter 通用契约与 Mock Server Adapter。

### 第二阶段：持久化恢复、控制与 Provider 对等

- Checkpoint 版本迁移、暂停/恢复、取消、超时；
- Wait Subscription、外部事件、Webhook、人工审批和 Timer；
- Tool Execution 幂等、退避和 Dead Letter；
- MySQL Provider 与跨 Provider 一致性测试；
- SSE 事件续读；
- 至少一个真实 Agent Server Adapter 和一个 DevOps Tool Adapter。

### 第三阶段：编排与平台化

- 子 Run、并行任务、Fan-out/Fan-in、Handoff 和条件分支；
- 多租户配额、预算与策略；
- Agent/Tool Registry、调试、重放、失败恢复；
- 按实际瓶颈引入缓存、消息分发和搜索能力。

## 10. 验收原则

系统至少应证明以下场景正确：

1. Worker 在执行中终止后，其他 Worker 能在 Lease 失效后安全继续。
2. 重复 Webhook、重复事件和重复 Task 投递不会产生重复状态推进或重复副作用。
3. 外部等待不占用 Worker，且事件到达后能从 checkpoint 恢复。
4. Task 完成时，事件、checkpoint、Run 投影和后续 Task 不会出现部分提交。
5. API、Scheduler、Worker 任意实例替换或扩容不会导致状态丢失。
6. PostgreSQL 与 MySQL Provider 能通过同一组并发、幂等、Lease 和故障恢复测试。
7. `cancel`、`pause` 与 Task 完成并发时，只能有一个合法状态推进，迟到结果无法覆盖新状态或 checkpoint。
8. 需求分析到 DevOps 部署的端到端示例可完成一次正常交付、一次人工暂停后恢复，以及一次测试失败返工。
9. Worker 在远程 Agent 提交前后崩溃时，不会创建不可追踪的重复远程 Run；事件流可从持久化游标续读。
