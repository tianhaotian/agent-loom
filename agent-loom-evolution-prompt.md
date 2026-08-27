# Agent Loom 后续演进执行 Prompt

> 用法：将下面的 Prompt 作为 Agent Loom 仓库中的长期主任务使用。每轮应只选择一个最小、完整、可验证的垂直切片，完成实现、测试和文档更新。

```text
你正在维护项目：

  ~/github/agent-loom

可参考的复杂应用场景和相关项目：

  OPC目标文档：
  ~/github/hermes-agent/output/markdown/opc-workflow-architecture-upgrade.md

  Hermes Runtime：
  ~/github/hermes-agent

你的任务是持续推进 Agent Loom 从“PostgreSQL 单租户工作流 MVP”演进为通用、持久化、事件驱动的 Agent Workflow Kernel。

重要：OPC 是 Agent Loom 的参考消费者和复杂验证场景，不是 Agent Loom 的产品规格。不要把 Agent Loom 实现成 OPC 专用流程引擎。

# 一、产品定位

Agent Loom 的目标是：

“为 Agent、工具和外部服务提供持久化、可恢复、可调度、可审计的事件驱动工作流运行内核。”

Agent Loom 应提供：

1. Durable Execution Kernel
   - Run、Task、Attempt
   - Event Ledger
   - Checkpoint
   - Lease、Fence、Generation
   - Command Receipt 和幂等处理
   - Retry、Deadline、Timeout
   - Pause、Resume、Cancel
   - Worker 崩溃恢复
   - 外部执行 Reconcile
   - At-least-once execution

2. 通用执行计划
   - ExecutionPlan
   - PlanRevision
   - TaskSpec
   - Dependency
   - Condition
   - JoinPolicy
   - RetryPolicy
   - TimeoutPolicy
   - ExtensionPayload
   - 动态任务和运行时计划修订

3. 通用调度
   - API Trigger
   - Event Trigger
   - External Signal
   - Delayed Task
   - Retry
   - Deadline
   - Cron/Schedule
   - Child Run Completion

4. Runtime Adapter
   - probe_capabilities
   - submit
   - get_status
   - read_events
   - request_stop
   - reconcile
   - resume
   - query_artifacts

5. 通用上下文机制
   - ContextSnapshot
   - ContextReference
   - ContextPatch
   - ContextProjection
   - ContextMergeStrategy
   - ContextLineage

6. Artifact、Wait、Outbox、审计和可观测性基础能力。

Agent Loom 不应该内置：

- OPC 的业务流程和阶段名称
- 京ME或AI代理语义
- OPC 固定的 Workflow Definition Schema
- OPC 特有的审批和发布流程
- scope_manifest、knowledge_package、artifact_spec 等固定业务字段
- OPC 的组织、角色和通知逻辑
- Hermes 特有的对话、Prompt 或 Toolset 逻辑
- 任何单一第三方 Runtime 的专用字段

这些内容应由上层服务、插件或独立 Integration/Adapter 提供。

# 二、核心设计原则

实施任何功能前，必须使用以下问题检查抽象是否合理：

“去掉 OPC、Hermes 或其他具体产品名称后，这项能力是否仍适用于至少两类不同工作流场景？”

如果答案是否定的，该能力不应进入 Agent Loom Core。

遵守以下规则：

1. Core 保持最小化。
2. 业务能力放在 Integration 或 Adapter 边缘。
3. 不为未来假设创建没有真实消费者的 SPI。
4. 新扩展接口必须至少有一个真实实现和行为测试。
5. 不把固定 DAG 作为唯一工作流模型。
6. 保留运行中动态创建任务和 Plan Revision 的能力。
7. Runtime 可以提出任务或计划变更，但正式状态变更必须由 Agent Loom 持久化和提交。
8. 数据库是执行状态和事件账本的事实来源。
9. 内存队列、通知和消息中间件只能用于加速，不能作为正确性来源。
10. 外部执行采用 at-least-once + idempotency，不承诺 exactly-once。
11. 所有外部副作用必须考虑提交前崩溃、提交后崩溃、响应丢失和重复调用。
12. 不用 TODO、空 Trait 或只定义不接线的类型来冒充功能完成。
13. 不破坏已有 PostgreSQL MVP 的事务、Lease、Fence 和 Receipt 不变量。
14. 测试应验证行为关系和竞态，不冻结无意义的常量或枚举数量。

# 三、当前项目水位

当前仓库处于：

- Phase 0 设计基本冻结
- PostgreSQL 单租户 MVP 垂直切片可运行
- PostgreSQL DurableStore、事务状态转换、Lease、Fence、Receipt 已有基础
- 已有 `ExecutionPlan V1` 到 `CreateRun` 的通用初始 Stage/Task 实例化路径，以及覆盖初始、动态后继、Wait 恢复 Task 的版本化 Handler 信封；Workflow Worker 已由注册表驱动领取和执行，当前只注册 delivery Handler
- 固定八阶段推进已隔离在 delivery Handler，server 仍以该 Handler 和 Mock Adapter 作为默认 MVP fixture
- MySQL 只有 migration，没有真实 Store Provider
- P0 要求的 Runtime Adapter `submit`、`read_events`、`get_status`、`reconcile_submission` 和 `request_stop` 均已接入真实执行路径；其余非 P0 Adapter 能力仍按具体集成扩展
- `submit`、提交结果未知时的 `reconcile_submission`、`read_events`、`request_stop` 和远端引用已知时的 `get_status` 已形成生产闭环，并覆盖持久化 event cursor、事件去重、重复 Worker cursor fencing、终态核验以及 Cancel 与提交响应竞态
- Worker 仍偏单进程 MVP
- 通用 External Signal/Wait 已持久化并支持匹配/单次消费/恢复；当前 HTTP E2E 重点覆盖 approval，更多信号类型可继续扩展验收
- Child Run、Fan-out 和显式 `all/any` Child Join Fan-in 核心语义已完成；后台自动触发 Child Join 属于生产调度增强，Handoff 属于 P2
- Transactional Outbox 已对所有权威 Event 形成事务写入、Lease 发布、失败重试和崩溃接管闭环，当前真实 Publisher 为结构化 JSON 日志
- P0 与 P1 清单中的核心能力均已完成。PlanRevision 已覆盖初始 revision、完整快照历史、HTTP 幂等提交、Run/Plan 双重 fencing、Event/Outbox 审计，以及 append-only 动态 Task 的同事务实例化；ExecutionPlan 已支持无环 Dependency、`all`/`any` JoinPolicy、成功/结果投影 Condition、事务内唯一激活和不可达分支递归 `skipped`。Context 已覆盖初始 Snapshot、replace/merge-patch、Run/Context fencing、不可变 Patch/Snapshot、父级 lineage，以及 Task 创建时固定的 ContextReference/JSON Pointer Projection；Artifact 引用/版本血缘已持久化；Child Run/Fan-out 已覆盖父 Run/Task 校验、幂等创建和直接子 Run 查询，显式 Child Join 可按 `all`/`any` 终态策略唯一激活父 Task。append-only 是 V1 动态计划的有意约束；动态 Stage 删除/改写、后台 Child Join 自动触发、Cron、Handoff、外部 Broker、MySQL 事务 Provider、多租户和生产可观测性属于 Phase 2B、P2 或更后续范围

首先检查当前代码和 git 状态，不要仅依赖上述描述。若描述已经过时，以当前代码和测试为准。

# 四、演进优先级

按以下顺序推进，但每次只选择一个最小、完整、可验证的垂直切片。

## P0：通用执行内核产品化

优先完成：

1. 移除核心运行路径对固定八阶段流程的依赖。
2. 支持从通用 ExecutionPlan/TaskSpec 创建 Run。
3. 完整区分 Run、Task、Attempt 和 ExternalExecution。
4. 补齐所有状态转换的事务边界和幂等规则。
5. 多进程 Worker 使用真正唯一的 worker_id。
6. 实现 Lease 续租、过期认领和 stale completion 拒绝。
7. 接通 Runtime Adapter 的：
   - submit
   - read_events
   - get_status
   - reconcile
   - request_stop
8. 实现持久化 external event cursor 和事件去重。
9. 实现通用 External Signal/Wait。
10. 实现 Transactional Outbox 或等价可靠事件发布机制。
11. 增加 PostgreSQL HTTP E2E 和多 Worker 竞态测试。

## P1：通用计划和上下文机制

在 P0 稳定后实现：

1. PlanRevision。
2. Dependency、Condition、JoinPolicy。
3. 动态任务变更的审计和版本化。
4. ContextSnapshot/Reference/Patch。
5. ContextProjection 和 ContextMergeStrategy。
6. Context Lineage。
7. Artifact 引用和血缘。
8. Child Run。
9. Fan-out/Fan-in。

不要直接把 OPC 的上下文字段做成核心字段。核心应保存通用、版本化的数据或 ExtensionPayload，由 Integration 解释具体语义。

## P2：通用调度和高级控制

实现：

1. Schedule/Cron。
2. timezone 和 DST。
3. misfire policy。
4. catch-up policy。
5. concurrency policy。
6. Retry/Fallback/Compensation。
7. Replay。
8. Replan。
9. Manual Intervention。
10. Handoff。

Schedule 触发必须有稳定的幂等键，例如：

  (schedule_id, scheduled_fire_time)

## P3：生产化和Provider扩展

在真实需求出现后实现：

1. 多租户隔离扩展点。
2. Authorization Provider。
3. Credential Resolver。
4. 分区和归档。
5. 调度分片。
6. 完整 metrics、trace 和运维接口。
7. MySQL DurableStore Provider。
8. 更多 Runtime Adapter。
9. 容量测试和故障注入。

MySQL、SQLite、WebSocket 不应自动成为 OPC 接入的前置阻塞项。

# 五、OPC和Hermes的使用方式

OPC应用于验证 Agent Loom 的抽象，而不是决定 Core 数据模型。

推荐边界：

- OPC：
  - 业务 Workflow Definition
  - Definition → ExecutionPlan 的业务编译
  - 产品态和用户态
  - SSO/RBAC
  - 京ME入口
  - 业务通知
  - OPC专用上下文语义

- Agent Loom：
  - 执行计划快照
  - Run/Task/Attempt
  - 调度、Lease、Retry、Wait
  - Event Ledger
  - Checkpoint
  - Runtime调用与恢复
  - 执行事实权威
  - Outbox和审计

- Hermes：
  - Agent Loop
  - 模型调用
  - Runtime内部工具
  - Runtime内部会话
  - 执行事件和Artifact输出

Hermes Adapter 应作为 Agent Loom 的第一个真实 Runtime Adapter，但不能让 Hermes 特有字段侵入 adapter-core 或 domain。

如果需要 OPC 特有映射，放到独立模块，例如：

  integrations/opc-agent-loom

如果需要 Hermes Adapter，放到独立模块，例如：

  adapters/adapter-hermes

# 六、本次执行流程

按照以下步骤执行：

1. 阅读：
   - README.md
   - REQUIREMENT.md
   - STATE_MACHINE.md
   - MVP.md
   - 当前 AGENTS.md
   - 相关 crate 和测试
   - 最近相关 git history

2. 检查：
   - git status
   - 当前测试状态
   - 当前实现是否已经覆盖本 Prompt 中的某些能力
   - 是否存在未提交的用户修改
   - 功能缺失是事实，还是当前设计的有意边界

3. 输出一个简短的 Current State Assessment：
   - 已实现
   - 部分实现
   - 未实现
   - 代码中已经存在但未接线
   - 当前最关键的技术风险

4. 从 P0 开始，选择“当前尚未完成的最高优先级最小垂直切片”。

5. 在编码前明确：
   - 该切片解决什么真实问题
   - 为什么它属于通用 Agent Loom 能力
   - 至少两个可适用场景
   - 状态机变化
   - 数据库事务边界
   - 幂等策略
   - 崩溃恢复策略
   - 不会进入 Core 的场景特有逻辑

6. 实现完整切片：
   - Domain
   - Store Trait
   - PostgreSQL Provider
   - Runtime/Server 接线
   - API（如果需要）
   - Migration（如果需要）
   - 行为测试
   - E2E测试
   - 文档更新

7. 不创建未使用的 Trait、字段、表或配置。
8. 不保留只供测试使用、生产路径未调用的“完成实现”。
9. 不为了测试方便绕开真实事务和真实 import 路径。
10. 若发现现有设计与需求冲突，先通过代码、测试和 git history 验证原始意图，再修改。

# 七、每个切片的完成标准

只有同时满足以下条件，才能声明完成：

- 生产执行路径已经调用新能力。
- PostgreSQL Provider 有真实实现。
- 状态转换具备原子性。
- 重试调用是幂等的。
- Worker 崩溃后可以恢复。
- 重复事件不会重复推进工作流。
- 过期 Worker 不能提交 stale completion。
- 有数据库无关的行为测试。
- 有 PostgreSQL 集成或 E2E测试。
- 文档准确反映当前能力和限制。
- 没有 OPC/Hermes 专用概念泄漏到 Core。
- 没有破坏现有测试和兼容性。

至少覆盖以下失败场景：

1. 创建 Run 请求重复。
2. Task Claim 后 Worker 崩溃。
3. 外部请求成功但响应丢失。
4. 外部事件重复或乱序。
5. Lease 过期后旧 Worker 返回结果。
6. Pause/Cancel 与 Complete 并发。
7. Wait 注册与外部事件同时发生。
8. 服务重启后继续已有 Run。
9. 数据库短暂不可用后恢复。

# 八、输出要求

每次迭代结束时输出：

1. 本次选择的垂直切片。
2. 为什么这是通用能力，而不是 OPC 定制。
3. 代码和数据库变更。
4. 新增或调整的行为不变量。
5. 测试命令和结果。
6. 尚未解决的风险。
7. 下一步建议，但不要在没有验证前声称后续能力已完成。

如果当前切片太大，应继续拆小，但不能拆成只有类型、没有运行链路的空实现。

现在开始：

- 先检查仓库当前状态和现有实现；
- 给出 Current State Assessment；
- 从 P0 中选择最高优先级、最小且能够端到端验证的切片；
- 完成设计、实现、测试和文档更新；
- 不要把 OPC 当作 Agent Loom 的唯一目标，也不要复制 OPC 的业务模型进入 Core。
```

后续继续推进时，可以在主 Prompt 末尾追加：

```text
基于当前仓库最新状态继续推进，不要重复已经完成的切片。优先处理上一轮报告中最高优先级、仍未闭环的问题。
```
