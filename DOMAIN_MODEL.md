# Agent Loom 领域模型与共享逻辑数据模型

## 1. 目的

本文将 [REQUIREMENT.md](./REQUIREMENT.md) 的产品对象和 [STATE_MACHINE.md](./STATE_MACHINE.md) 的状态语义落实为 PostgreSQL、MySQL 共同遵循的逻辑数据模型。

本文冻结实体职责、关系、关键字段、唯一约束、索引意图和跨表不变量，但不要求两个 Provider 使用逐字相同的 DDL、物理类型或迁移脚本。

## 2. 建模原则

### 2.1 领域边界

- Run 是一次 Workflow 或 Agent 的长期执行及控制边界。
- StageExecution 是业务进度与质量门禁边界。
- Task 是 Worker 可领取的有限执行边界。
- ToolExecution 和 AgentExecution 是外部副作用与远程执行边界。
- Event 是不可变事实和审计序列；Run、StageExecution、Task 等是当前状态投影。
- Checkpoint 是恢复快照，不代替 Event 审计，也不承载大文件本体。
- WaitSubscription 是外部条件的一次性消费边界。
- CommandReceipt 是 API、Webhook、Worker 提交和 Adapter 回调的幂等边界。

### 2.2 多租户

- 所有业务表必须包含 `tenant_id`，包括通过父表可以推导 tenant 的子表。
- 所有读写命令必须显式传入 tenant，Provider 查询不得仅以资源 ID 定位业务数据。
- 关键外键使用 `(tenant_id, resource_id)` 复合引用，防止错误代码建立跨 tenant 关系。
- 每个以单列 ID 为主键的 tenant 业务表还必须声明 `(tenant_id, id)` 候选唯一键，供复合外键引用。
- PostgreSQL RLS 等数据库专属能力可以作为纵深防御，但不得成为跨 Provider 正确性的唯一实现。
- 凭据只保存不透明 `credential_ref`，不得写入 Event、Checkpoint、CommandReceipt 或一般日志。

### 2.3 可移植逻辑类型

| 逻辑类型 | PostgreSQL 建议 | MySQL 8+ 建议 | 约束 |
| --- | --- | --- | --- |
| `Id` | `uuid` | `binary(16)` | 应用生成、全局唯一；建议使用时间有序的 128 位 ID |
| `Instant` | `timestamptz(6)` | `datetime(6)` | 一律按 UTC 解释，写入数据库权威时间 |
| `Json` | `jsonb` | `json` | 业务正确性不得依赖某一方言的 JSON 查询 |
| `Digest` | `bytea` | `binary(32)` | 默认 SHA-256，比较原始字节 |
| `Version` | `bigint` | `bigint` | 非负、单调递增 |
| `Status` | `varchar(32)` + check | `varchar(32)` + check | Rust enum 是公共语义，数据库约束是防御层 |
| `SecretRef` | `varchar(512)` | `varchar(512)` | 只保存凭据系统引用 |

- JSON 的 `request_hash`、Checkpoint digest 和定义版本 digest 必须基于应用层规范化编码计算，不能依赖数据库 JSON 字段顺序。
- 金额、Token 预算和配额不得使用浮点数；使用最小单位整数或定点小数。
- 所有 ID、状态和时间通过 Provider codec 转换，Runtime crate 不暴露数据库驱动类型。

## 3. 聚合与关系

```text
Tenant
├── WorkflowDefinition ── WorkflowDefinitionVersion
├── AgentDefinition ───── AgentDefinitionVersion
├── AgentEndpoint
└── Run
    ├── StageExecution
    │   ├── Task ── TaskAttempt
    │   │   ├── ToolExecution ── ToolExecutionAttempt
    │   │   └── AgentExecution ── AgentEventReceipt
    │   └── ArtifactRef
    ├── Event
    ├── Checkpoint
    ├── WaitSubscription
    ├── CommandReceipt
    └── OutboxMessage
```

Run 是主要事务聚合根。涉及 Run 状态推进的 Stage、Task、Wait、Checkpoint、Artifact 和 Event 写入必须通过 DurableStore 领域操作完成，不能由 Adapter 直接 CRUD。

## 4. 定义与配置模型

### 4.1 `tenants`

身份认证与组织管理可以由外部系统提供，但 DurableStore 需要最小 tenant 占位以建立复合外键和生命周期边界：

```text
tenant_id, tenant_key, status, policy_json, created_at, updated_at
```

- 唯一：`tenant_id`、`tenant_key`；
- `status` 至少支持 `active/suspended/deleting`；
- tenant suspended 后不得创建或领取新任务，但已有数据仍可按授权查询；
- tenant 物理删除属于独立合规流程，不通过普通 Runtime 命令执行。

### 4.2 `workflow_definitions`

保存 Workflow 的稳定身份和管理状态。

| 字段 | 逻辑类型 | 说明 |
| --- | --- | --- |
| `workflow_id` | Id | 主键 |
| `tenant_id` | Id | tenant 边界 |
| `workflow_key` | string | tenant 内稳定业务键 |
| `name` | string | 展示名称 |
| `status` | Status | `active/archived` |
| `latest_version` | Version? | 查询加速，不作为 Run 的版本依据 |
| `created_at/updated_at` | Instant | 审计时间 |

约束：

- 主键：`workflow_id`；
- 唯一：`(tenant_id, workflow_key)`；
- 复合候选键：`(tenant_id, workflow_id)`。

### 4.3 `workflow_definition_versions`

| 字段 | 逻辑类型 | 说明 |
| --- | --- | --- |
| `workflow_version_id` | Id | 主键 |
| `tenant_id/workflow_id` | Id | 所属 Workflow |
| `version` | Version | 业务版本号 |
| `lifecycle` | Status | `draft/published/retired` |
| `spec_json` | Json | 阶段骨架、角色、门禁、默认策略 |
| `spec_digest` | Digest | 规范化定义摘要 |
| `created_by` | string | actor 引用 |
| `created_at/published_at` | Instant | 生命周期时间 |

- 唯一：`(tenant_id, workflow_id, version)`、`(tenant_id, workflow_version_id)`。
- Run 只能引用 `published` 版本。
- 版本发布后 `spec_json` 与 `spec_digest` 不可修改；修订必须创建新版本。
- 动态 Stage/Task 可以在 Run 内产生，但必须记录生成它的 Event 和基础 Workflow 版本。

首个可执行 Profile 为 `agent-loom.execution-plan/v1`：

```json
{
  "schema": "agent-loom.execution-plan/v1",
  "plan_key": "research",
  "stages": [
    {"key": "discovery", "activation": "active"}
  ],
  "initial_tasks": [
    {
      "key": "collect-sources",
      "stage_key": "discovery",
      "kind": "agent_server",
      "max_attempts": 3,
      "input": {"capability": "research"}
    }
  ],
  "extension": {}
}
```

- V1 只描述创建 Run 时需要原子实例化的初始 Stage 和 Task，不把整个运行限制为固定 DAG。
- `initial_tasks` 至少包含一个 Task；Task key 在计划内唯一，引用的初始 Stage 必须存在且为 `active`。
- Task 的静态 `input` 与创建 Run 的 `run_input` 以独立字段组成持久化输入信封，避免 Core 解释业务字段或字符串模板。
- `extension` 是不透明、版本化 JSON；其业务语义由 Integration/Adapter 解释。
- 同一个已发布 Workflow Version 的计划不可修改；结构变化必须发布新版本，已有 Run 继续引用原版本。

### 4.4 `agent_definitions` 与 `agent_definition_versions`

采用与 Workflow 相同的“稳定身份 + 不可变版本”模式。

版本至少保存：

```text
agent_version_id, tenant_id, agent_id, version, lifecycle,
system_instructions, model_config_json, tools_json, capabilities_json,
handoff_json, guardrails_json, limits_json, spec_digest, created_at
```

Run、StageExecution 或 Task 必须引用确切 `agent_version_id`，不能只引用会漂移的 `agent_id`。

### 4.5 `agent_endpoints`

保存远程 Agent Server 的连接元数据，不保存明文密钥。

```text
endpoint_id, tenant_id, endpoint_key, adapter_kind, base_uri,
protocol_version, capabilities_json, credential_ref,
status, health_checked_at, created_at, updated_at
```

- 唯一：`(tenant_id, endpoint_key)`。
- `base_uri` 必须经过 SSRF 策略与网络边界校验。
- 能力发现结果是缓存；每个 AgentExecution 还需保存提交时的能力快照。

## 5. Run 聚合

### 5.1 `runs`

| 字段 | 逻辑类型 | 说明 |
| --- | --- | --- |
| `run_id` | Id | 主键 |
| `tenant_id` | Id | tenant 边界 |
| `workflow_version_id` | Id? | 顶层 Workflow 版本 |
| `coordinator_agent_version_id` | Id? | 协调 Agent 版本 |
| `parent_run_id` | Id? | 子 Run 的父 Run |
| `parent_task_id` | Id? | 创建子 Run 的 Task |
| `status` | Status | Run 状态机状态 |
| `suspended_from_status` | Status? | Pause 前投影，仅用于审计与恢复参考 |
| `version` | Version | CAS 版本，每次业务状态转换递增 |
| `execution_generation` | Version | Pause/Cancel/Resume 的执行栅栏 |
| `next_event_sequence` | Version | 下一个 Event 序号 |
| `current_checkpoint_id` | Id? | 最近成功 Checkpoint |
| `terminal_event_id` | Id? | 唯一终态 Event |
| `input_json` | Json | 规范化输入，不含凭据 |
| `state_summary_json` | Json | 可查询的小型当前摘要，不是完整 Checkpoint |
| `deadline` | Instant? | 全局截止时间 |
| `resume_blocked_reason` | string? | 未知副作用等恢复阻塞原因 |
| `created_by` | string | actor/producer |
| `created_at/updated_at/terminal_at` | Instant | 生命周期时间 |

关键约束：

- 主键：`run_id`；候选键：`(tenant_id, run_id)`。
- `version >= 0`、`execution_generation >= 0`、`next_event_sequence >= 1`。
- `terminal_event_id` 为空或只指向本 Run Event。
- 终态必须有 `terminal_event_id` 和 `terminal_at`；非终态不得设置 `terminal_at`。
- `parent_run_id` 不得等于 `run_id`；更深层循环由创建子 Run 的领域操作检查。

推荐索引：

- `(tenant_id, status, updated_at, run_id)`：控制台分页；
- `(tenant_id, workflow_version_id, created_at, run_id)`：Workflow 查询；
- `(tenant_id, parent_run_id, created_at)`：子 Run 查询；
- `(status, deadline, run_id)`：Scheduler 扫描 deadline。

## 6. 业务阶段与产物

### 6.1 `stage_executions`

```text
stage_execution_id, tenant_id, run_id, stage_key,
definition_stage_key, parent_stage_execution_id, generated_by_event_id,
status, version, attempt, assignee_kind, assignee_ref,
agent_version_id, input_contract_json, output_contract_json,
policy_json, started_at, completed_at, created_at, updated_at
```

- `stage_key` 是 Run 内逻辑阶段身份，例如 `requirements`、`prd`、`implementation`。
- 静态阶段设置 `definition_stage_key`；动态生成阶段可以为空，但必须设置 `generated_by_event_id`。
- 唯一：`(tenant_id, run_id, stage_key, attempt)`。
- `version` 用于阶段级 CAS；状态语义遵循 `STATE_MACHINE.md`。
- 返工可以复用 stage_key 并递增 attempt；`parent_stage_execution_id` 指向触发返工或派生的阶段实例。

推荐索引：

- `(tenant_id, run_id, status, stage_key)`；
- `(tenant_id, assignee_kind, assignee_ref, status, updated_at)`。

### 6.2 `artifact_refs`

```text
artifact_id, tenant_id, run_id, stage_execution_id, task_id,
logical_key, kind, contract_version, version, uri, digest, media_type,
size_bytes, source_artifact_refs_json, metadata_json, produced_by,
created_event_id, created_at
```

- ArtifactRef 只保存引用与完整性元数据，不默认保存代码、设计稿、构建包等大对象。
- 唯一：`(tenant_id, run_id, logical_key, version)`。
- `digest` 基于产物内容或外部系统可验证摘要。
- `contract_version` 固定结构化产物 schema；`source_artifact_refs_json` 保存确定版本的数据血缘引用。
- ArtifactRef 创建后不可原地覆盖；新内容创建新 version。
- 首个业产研场景的 logical key、需求等价模型、产物 schema 与追踪约束以 [E2E_SCENARIO.md](./E2E_SCENARIO.md) 为准。
- URI 返回客户端前必须经过授权检查，敏感 URI 应使用短期签名或间接资源 ID。

推荐索引：

- `(tenant_id, run_id, stage_execution_id, kind, version)`；
- `(tenant_id, digest)` 用于可控范围内的去重或完整性查询。

## 7. Task 与 Worker 执行

### 7.1 `tasks`

| 字段 | 说明 |
| --- | --- |
| `task_id, tenant_id, run_id, stage_execution_id` | 身份与归属 |
| `logical_key` | Run 内稳定任务键，用于防止重复生成后续 Task |
| `kind` | model、tool、agent_server、artifact_check、timer_wakeup 等 |
| `status` | Task 状态机状态 |
| `generation` | 必须等于 Run execution_generation 才可领取/完成 |
| `based_on_checkpoint_sequence` | 生成 Task 时所依据的 Checkpoint |
| `priority` | 同一调度域内的领取优先级 |
| `available_at` | 可领取时间 |
| `attempt/max_attempts` | 尝试次数与上限 |
| `lease_owner/lease_token/lease_expires_at` | 当前 Lease |
| `input_json/result_json` | 有界输入与结果摘要；新 Task 的输入使用版本化 Handler 信封 |
| `error_code/error_json` | 结构化错误，不含凭据 |
| `deadline` | Task 截止时间 |
| `created_event_id` | 创建 Task 的因果 Event |
| `created_at/updated_at/completed_at` | 生命周期时间 |

唯一约束：

- `(tenant_id, task_id)`；
- `(tenant_id, run_id, logical_key, generation)`，防止同一转换重复生成等价 Task。

领取索引必须服务以下谓词：

```text
task.status = queued
AND task.available_at <= database_now
AND tenant.status = active
AND run.status IN (queued, running)
AND task.generation = run.execution_generation
AND run.deadline > database_now（如果存在）
```

建议索引：

- 全局领取：`(status, available_at, priority, task_id)`；
- tenant 调度：`(tenant_id, status, available_at, priority, task_id)`；
- Lease 回收：`(status, lease_expires_at, task_id)`；
- Run 查询：`(tenant_id, run_id, status, created_at)`。

Provider 可以调整 priority 的物理排序方向，但领取结果必须遵守统一排序与公平性策略。

新建 Task 的 `input_json` 使用 `agent-loom.task-input/v1` 信封，至少包含稳定的
`handler` logical key 和 Handler 自己解释的 `payload`。ExecutionPlan 只负责把静态
TaskSpec 与 Run input 绑定到该信封；动态后继 Task 和 Wait 恢复 Task 必须继续保留同一
Handler key。通用 Workflow Worker 从注册表汇总可领取的 Task kind，再按持久化
Handler key 把 Lease、不可变输入和领取后的 Run fence 交给对应 Handler；注册表拒绝
重复 Handler key 和没有支持 kind 的 Handler。Worker 不能用 `kind = NULL` 的全类型
领取吞掉维护或恢复 Worker 的 Task。当前同一 tenant 调度域中的 Workflow Worker 必须
注册它所领取 kind 对应的全部 Handler；若未来需要按 Handler 拆分专用 Worker pool，
必须先把 Handler key 提升为数据库可过滤的领取维度。升级期间允许 delivery Handler
读取旧版无信封的 delivery Task，但所有新写入都必须使用 V1 信封。

### 7.2 `task_attempts`

每次领取追加一行，并在同一 Lease 所有权下至多完成一次 finalize：

```text
task_attempt_id, tenant_id, task_id, run_id, attempt,
worker_id, lease_token_digest, claimed_at, lease_expires_at,
finished_at, outcome, error_code, metrics_json
```

唯一：`(tenant_id, task_id, attempt)`。Lease token 只保存摘要用于审计，当前有效 token 保存在 Task 热行中。领取时 `finished_at/outcome` 为空；完成、失败、Lease 过期回收或取消时，以 `finished_at IS NULL` 条件更新一次。finalize 后禁止再次修改，普通 Runtime 禁止删除。

## 8. Event、Checkpoint 与幂等

### 8.1 `events`

```text
event_id, tenant_id, run_id, sequence, event_type, payload_json,
payload_schema_version, producer, actor_ref,
correlation_id, causation_id, idempotency_key,
occurred_at, recorded_at
```

- 唯一：`(tenant_id, run_id, sequence)`、`(tenant_id, event_id)`、`(tenant_id, run_id, event_id)`。
- Event 创建后禁止 UPDATE；归档与合规删除走独立管理流程。
- `occurred_at` 是生产者声明时间，`recorded_at` 是数据库接收时间；状态裁决使用后者和数据库当前时间。
- `causation_id` 指向导致本 Event 的 Event/Command；外部根事件可以为空。
- payload 必须使用版本化 schema，并在写入前执行敏感字段过滤。

查询索引：

- `(tenant_id, run_id, sequence)`：权威分页与 SSE 补读；
- `(tenant_id, correlation_id, recorded_at)`：链路查询；
- `(tenant_id, event_type, recorded_at)`：审计筛选。

初始版本不对 events 做物理分区。后续若按时间分区，必须保留全局 `(tenant_id, run_id, sequence)` 唯一性，可使用独立 `event_keys` 唯一守卫表或 Provider 等价机制，并在同一事务写入。不得为了分区牺牲事件顺序约束。

### 8.2 `checkpoints`

```text
checkpoint_id, tenant_id, run_id, sequence,
schema_version, workflow_version_id, coordinator_agent_version_id,
execution_generation, state_json, state_digest,
created_event_id, created_at
```

- Checkpoint 只追加、不原地更新。
- 唯一：`(tenant_id, run_id, sequence)`。
- 候选唯一：`(tenant_id, checkpoint_id)`、`(tenant_id, run_id, checkpoint_id)`，供复合 FK 证明 tenant/Run 归属。
- `runs.current_checkpoint_id` 只能前进到更大 sequence，普通恢复不得回退。
- 显式重放或管理员恢复必须产生新 Checkpoint 和审计 Event，不能直接修改 current 指针制造无记录回退。
- `state_digest` 用于检测损坏和错误迁移，不作为安全签名。

### 8.3 `command_receipts`

```text
receipt_id, tenant_id, scope, idempotency_key, request_hash,
outcome_kind, outcome_json, event_id, resource_type, resource_id,
resource_version, created_at, expires_at
```

- 唯一：`(tenant_id, scope, idempotency_key)`。
- Receipt 与业务转换在同一事务提交。
- 相同 key、不同 `request_hash` 返回 `IDEMPOTENCY_KEY_REUSED`。
- `outcome_json` 必须足以重建首次 API/Worker 命令的领域结果，但不得保存凭据或未过滤的大 payload。
- 清理 Receipt 前必须确认所有调用方的最大重试窗口已结束。

## 9. 等待模型

### 9.1 `wait_subscriptions`

```text
wait_id, tenant_id, run_id, stage_execution_id,
wait_type, expected_event_type, match_key_hash,
match_contract_json, status, active_slot,
expires_at, consumed_by_event_id, created_event_id,
created_at, consumed_at, updated_at,
resume_task_id, resume_logical_key, resume_task_kind,
resume_priority, resume_max_attempts, resume_input_json, resume_deadline
```

`match_key_hash` 用于索引匹配；原始敏感匹配值应加密保存或只保存在受控外部系统。

`resume_*` 字段是在 Wait 创建事务中冻结的恢复 Task 计划。外部事件只能消费 Wait 并实例化该计划，不能直接指定任意后继 Task；暂停期间到达的事件会生成受控制门阻塞的 `scheduled` Task。

为兼容不支持部分唯一索引的 Provider，使用 `active_slot` 表达“只允许一个 open 等价等待”：

- open 时 `active_slot = 1`；
- consumed/expired/cancelled 时 `active_slot = NULL`；
- 唯一：`(tenant_id, run_id, wait_type, expected_event_type, match_key_hash, active_slot)`。

PostgreSQL 与 MySQL 都允许唯一约束中存在多个 NULL，因此历史终态记录不会互相冲突。Provider 仍必须以状态条件更新保证单次消费。

推荐索引：

- 事件匹配：`(tenant_id, status, expected_event_type, match_key_hash)`；
- 超时扫描：`(status, expires_at, wait_id)`；
- Run 查询：`(tenant_id, run_id, status, created_at)`。

## 10. 外部执行模型

外部协议、能力快照、事件映射和 Adapter 错误语义以 [ADAPTER_CONTRACT.md](./ADAPTER_CONTRACT.md) 为准；本节只定义持久化字段与约束。

### 10.1 `tool_executions`

```text
tool_execution_id, tenant_id, run_id, stage_execution_id, task_id,
tool_call_id, tool_name, idempotency_scope, idempotency_key, request_hash,
status, attempt_count, request_json, result_json,
error_code, recovery_action, external_ref, retry_at,
started_at, completed_at, updated_at
```

- 唯一：`(tenant_id, tool_call_id)`；
- 唯一：`(tenant_id, idempotency_scope, idempotency_key)`；Adapter 必须稳定定义 scope，不能依赖调用方临时拼接；
- 已 succeeded 的调用不得再次执行；
- `outcome_unknown` 必须设置 recovery_action 或进入人工队列。
- `retry_scheduled` 必须设置数据库时间语义的 `retry_at`；其他状态不得残留该字段。

`tool_execution_attempts` 在外部调用前追加请求开始记录，并在同一 attempt 下以 `request_finished_at IS NULL` 条件 finalize 一次，保存结束时间、Adapter 错误分类、外部 request ID 与响应摘要。finalize 后不可再次修改。重试使用同一 ToolExecution 和幂等键，不创建新的逻辑调用；每个新 attempt 必须由匹配 `tool.retry_due` Event 的已领取恢复 Task 在启动事务中追加。

### 10.2 `agent_executions`

```text
agent_execution_id, tenant_id, run_id, stage_execution_id, task_id,
endpoint_id, agent_version_id, idempotency_key, request_hash, request_json,
remote_run_ref, remote_session_ref, status, version,
capabilities_snapshot_json, event_cursor, cursor_version,
stop_requested_at, stop_outcome, result_json, error_code,
retry_at, last_synced_at, created_at, updated_at, completed_at
```

约束：

- 唯一：`(tenant_id, agent_execution_id)`；
- 唯一：`(tenant_id, endpoint_id, idempotency_key)`；
- 非空 remote reference 时应在 Endpoint 作用域唯一；
- event cursor 更新必须匹配 `cursor_version`，避免两个同步 Worker 互相覆盖；
- `request_json` 是规范化、可重放的 Agent Server 请求信封；必须与 `request_hash` 一致并在首次提交前持久化，凭据只能保存引用，不能进入请求信封；
- `SameRequestBackoff` 映射到 `reconciling` 并必须设置数据库时间语义的 `retry_at`；其他 Agent 状态不得残留该字段；
- `reconciling → submitting` 的自动重提必须由匹配 `agent.retry_due` Event 的已领取恢复 Task 授权，且沿用原 Endpoint 与 idempotency key；
- capabilities snapshot 创建后不可覆盖，能力重新发现只影响新 AgentExecution。

远程 Agent 进入 running 后，提交 Task 创建 WaitSubscription 或短时 poll Task，并释放 Worker。AgentExecution 的 running 不要求 Run 保持 running；Run 可以投影为 waiting。

### 10.3 `agent_event_receipts`

远程 Event 去重不能依赖 JSON 内 vendor 字段，使用独立守卫表：

```text
agent_event_receipt_id, tenant_id, agent_execution_id, run_id,
dedupe_key, source_event_id, source_sequence, source_cursor,
event_kind, raw_digest, local_event_id, recorded_at
```

- 唯一：`(tenant_id, agent_execution_id, dedupe_key)`；
- 候选唯一：`(tenant_id, agent_event_receipt_id)`；
- `dedupe_key` 是 Adapter 根据远程 event ID，或 sequence/cursor + kind + canonical payload digest 生成的固定摘要；
- 同一 dedupe key 但 raw digest 不同属于协议冲突，不得视为普通 duplicate；
- receipt、本地 Event、AgentExecution cursor 和衍生 Artifact/Wait/Task 在同一事务提交；
- Adapter/Runtime 必须把衍生动作编码为显式 `AgentEventProjection`；Provider 不解析 vendor payload 推断工作流动作；
- 只有新接收的权威事件可以携带投影；投影对象的 `created_event_id` 必须等于该事件的本地 Event ID；
- transient/ignored 远程事件可以没有 `local_event_id`，但影响状态的事件必须关联本地 Event。

## 11. Transactional Outbox

`outbox_messages` 为每个权威 Event 保存可靠发布意图；外部 Broker 可以按部署需要替换当前 Publisher：

```text
outbox_id, tenant_id, event_id, topic, partition_key,
payload_json, status, attempt, available_at,
lease_owner, lease_token, lease_expires_at,
created_at, published_at
```

- Event 与 OutboxMessage 在同一业务事务创建。
- 唯一：`(tenant_id, event_id, topic)`，防止重复产生同一投递意图。
- 发布语义仍为 at-least-once；消费者必须以 event_id 或领域幂等键去重。
- 发布失败只推进 Outbox 的重试状态，不得回滚已经提交的 Run 状态。

## 12. 跨表不变量

以下规则必须由 DurableStore 事务和黑盒测试共同保证：

1. 所有关联对象的 tenant 必须与 Run 一致。
2. Run version、Event sequence、Checkpoint sequence 和 execution generation 单调不减。
3. 一个 Run 只有一个 terminal Event，终态不可逆。
4. 一个 Task 只有一个成功 completion，旧 Lease 和旧 generation 不能推进状态。
5. 一个 WaitSubscription 只能被一个 Event 消费。
6. 一个幂等作用域和 key 只对应一个 request hash 和确定结果。
7. Stage succeeded 必须满足 Artifact Contract，Task succeeded 不能直接替代阶段完成判定。
8. 当前 Checkpoint、terminal Event、Stage、Task、Wait、Artifact 和 Execution 必须属于同一 Run。
9. Pause/Cancel 后的远程迟到结果可以被记录，但不能更新新 generation 的业务投影。
10. ToolExecution/AgentExecution 的 outcome_unknown 未解决时，普通 Resume 不得继续可能重复副作用的路径。
11. 同一 AgentExecution 的远程 dedupe key 只能对应一个 raw digest 和至多一个本地 Event。

数据库 CHECK、UNIQUE 和 FK 只承担单表或简单引用防御；涉及多个当前状态的规则必须由领域事务实现。不得把核心业务状态机隐藏在仅一个 Provider 拥有的触发器或存储过程中。

## 13. 删除、保留与隐私

- Definition 可以 archived，但被 Run 引用的版本不得物理删除。
- Event、Checkpoint、CommandReceipt 和执行审计按 tenant 策略保留；清理操作必须有管理审计。
- ArtifactRef 删除不等于外部产物删除；需要通过 Tool Adapter 执行外部删除并记录结果。
- 涉及隐私删除时，优先对敏感 payload 做加密密钥销毁、字段擦除或受控重写副本，不能无审计地 UPDATE 不可变 Event。
- 大表归档后，Run 查询应明确返回 `archived` 数据位置或受限结果，不能静默表现为事件缺失。

## 14. 迁移与 Provider 目录

建议迁移布局：

```text
crates/
  domain/                  # 领域 ID、状态、命令、错误；无 SQL 驱动依赖
  durable-store/           # DurableStore trait 与一致性测试套件
  store-postgres/
    migrations/
  store-mysql/
    migrations/
```

- 两个 Provider 使用独立物理迁移，但共享逻辑 migration ID 和模型变更说明。
- 采用 expand → backfill → switch → contract，避免滚动部署期间新旧实例互相破坏。
- 新增状态值时先扩展数据库约束，再发布会写入该状态的 Runtime。
- 删除字段前必须确认所有 Checkpoint schema 和旧 Worker 版本不再读取。
- 每次迁移后运行 Provider 黑盒一致性测试和最小业产研 E2E 场景。

## 15. 首批迁移建议

为降低初始闭环复杂度，建议按以下顺序创建：

1. tenant 占位、Workflow/Agent Definition 与版本表；
2. runs、events、command_receipts；
3. stage_executions、tasks、task_attempts、checkpoints；
4. wait_subscriptions、artifact_refs；
5. tool_executions/attempts、agent_endpoints、agent_executions、agent_event_receipts；
6. outbox_messages（仅在实际引入消息系统时）。

首批迁移不启用事件时间分区、数据库专属通知、全文搜索或复杂 JSON 索引。先通过状态、幂等、Lease、故障恢复和 PostgreSQL/MySQL 对等测试，再根据数据量增加物理优化。

## 16. 验收清单

- 每个业务表都有 tenant 边界和对应 tenant 查询索引。
- 所有跨表关系均能证明不会关联到其他 tenant。
- Run、Event、Task、Wait 和 CommandReceipt 的唯一约束能阻止重复状态推进。
- Task claim、Lease 回收、Event 分页、Run 控制台和 Wait 超时均有匹配索引。
- Checkpoint、Workflow Version、Agent Version 和 Artifact 都是版本化引用。
- Tool/Agent 外部执行可以表示 `outcome_unknown`，且不会被普通重试绕过。
- PostgreSQL 与 MySQL Provider 不向 Runtime 暴露方言类型或 SQL 特性。
- 两个 Provider 对同一命令历史返回相同领域状态、版本、错误分类和事件顺序。

具体物理类型、DDL 批次、循环外键、事务隔离、Task 领取和在线迁移规则以 [MIGRATION_DESIGN.md](./MIGRATION_DESIGN.md) 为准。
