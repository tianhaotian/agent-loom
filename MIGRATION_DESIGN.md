# Agent Loom PostgreSQL / MySQL 对等迁移设计

## 1. 目的与适用范围

本文把 [DOMAIN_MODEL.md](./DOMAIN_MODEL.md) 的共享逻辑模型落实为 PostgreSQL 与 MySQL/InnoDB 两套可独立演进、领域语义对等的物理迁移方案，并约束：

- 表、字段、主键、外键、唯一约束和索引意图；
- 数据库类型、排序规则、时间和 JSON 的可移植映射；
- Task 领取、Lease、Event 顺序、Wait 消费和终态竞争所需的锁/CAS 模板；
- expand → backfill → switch → contract 的在线演进方式；
- 两个 Provider 必须共享的 migration ID、能力预检和黑盒验收。

本文不是可直接运行的完整 DDL。正式 SQL 分别进入：

```text
crates/store-postgres/migrations/
crates/store-mysql/migrations/
```

两个目录允许使用不同物理语法，但必须实现相同逻辑 migration、约束结果和 DurableStore 行为。Runtime、domain 和 adapter crate 不得依赖任一数据库驱动类型或方言 SQL。

## 2. 官方能力基线与支持策略

### 2.1 必需能力

生产 Provider 启动前必须验证：

| 能力 | PostgreSQL | MySQL |
| --- | --- | --- |
| 事务存储与行锁 | MVCC + row lock | InnoDB |
| 队列竞争优化 | `FOR UPDATE ... SKIP LOCKED` | `FOR UPDATE ... SKIP LOCKED` |
| 强制 CHECK | 支持 | 必须为 enforced CHECK 的版本 |
| JSON | `jsonb` | `json` |
| 微秒时间 | `timestamptz(6)` | `datetime(6)` |
| 复合唯一/FK | 支持 | 支持，且所有表必须为 InnoDB |
| UTF-8 | database encoding UTF8 | `utf8mb4` |

MySQL CHECK 必须实际 enforced，不能以 `NOT ENFORCED` 创建；能力预检和 schema snapshot 都要读取约束状态：[MySQL CHECK Constraints](https://dev.mysql.com/doc/refman/8.4/en/create-table-check-constraints.html)。

PostgreSQL 的 locking clause 和 MySQL/InnoDB 的 locking read 都支持 `SKIP LOCKED`；两者官方文档也都强调它返回的是跳过已锁记录的不一致视图，因此只适用于队列式竞争，不能用来证明一般业务状态正确性：[PostgreSQL SELECT](https://www.postgresql.org/docs/current/sql-select.html)、[MySQL Locking Reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)。

参考 CI 首先覆盖 PostgreSQL 16+ 与 MySQL 8.4 LTS。其他仍受维护的 PostgreSQL/MySQL 8.x 版本只有在通过 capability probe、迁移测试和完整 Provider conformance suite 后才能列入发布支持矩阵；不得仅比较版本字符串后宣称兼容。

### 2.2 禁止依赖的专属能力

以下能力可以作为可选优化，但不能成为正确性前提：

- PostgreSQL partial index、RLS、LISTEN/NOTIFY、advisory lock、`RETURNING` 组合写法；
- MySQL generated-column JSON index、named lock、Event Scheduler、特定 online DDL algorithm；
- 任一数据库的触发器、存储过程、枚举类型、自动 ID 或数据库内 JSON 业务逻辑；
- statement-based replication 下的 `SKIP LOCKED` 行为。

Provider 可以使用专属 SQL 缩短事务或提高性能，但必须提供等价路径，并用相同黑盒测试证明领域结果一致。

## 3. 物理命名与类型映射

### 3.1 命名约定

- 表、字段、索引和约束使用 `snake_case` ASCII。
- 所有约束显式命名，格式为 `pk_*`、`uq_*`、`fk_*`、`ck_*`。
- MySQL CHECK 名称在 schema 范围保持唯一，统一带表名前缀。
- 普通索引命名为 `ix_<table>__<purpose>`，不把全部列名机械拼入名称。
- 逻辑 migration ID 使用四位序号和稳定名称，例如 `0003_run_core`。
- 生产表不使用 SQL 保留词作为裸标识符，不依赖 quoted identifier。

### 3.2 类型矩阵

| 逻辑类型 | PostgreSQL | MySQL/InnoDB | 应用约束 |
| --- | --- | --- | --- |
| `Id` | `uuid` | `binary(16)` | 应用生成 128-bit 时间有序 ID；按原始字节排序 |
| `Instant` | `timestamptz(6)` | `datetime(6)` | UTC；Provider 连接初始化验证时区 |
| `Json` | `jsonb` | `json` | 应用层 schema 校验和 canonical digest |
| `Digest` | `bytea` | `binary(32)` | SHA-256 原始 32 字节 |
| `Version` | `bigint` | `bigint` | signed、非负；不使用 unsigned 暴露差异 |
| `Boolean` | `boolean` | `boolean`/`tinyint(1)` | MySQL 增加值域 CHECK |
| `Status` | `varchar(32)` | `varchar(32)` | binary/code-point 比较，Rust enum 为权威 |
| `Key` | `varchar(255)` | `varchar(255)` | identity key 使用大小写敏感排序规则 |
| `ShortText` | `varchar(512)` | `varchar(512)` | 不保存秘密原文 |
| `LongText` | `text` | `text`/`longtext` | 不进入复合唯一索引 |
| `Counter` | `integer/bigint` | `integer/bigint` | 非负 CHECK |

MySQL 选择 `datetime(6)` 而不是 `timestamp`，避免较窄时间范围和连接时区自动转换产生隐式行为；官方文档说明 `TIMESTAMP` 会按会话时区转换且存在范围限制，而 `DATETIME` 不做该转换：[MySQL 日期时间类型](https://dev.mysql.com/doc/refman/8.4/en/datetime.html)。Provider 每次建立连接必须执行/验证 `time_zone = '+00:00'`，并只绑定 UTC 值。

### 3.3 字符集与排序规则

PostgreSQL 数据库必须使用 UTF8，身份键列显式使用确定性的 `C` collation；MySQL schema 默认：

```sql
DEFAULT CHARACTER SET utf8mb4
DEFAULT COLLATE utf8mb4_0900_bin
```

约束：

- `tenant_key`、`workflow_key`、`logical_key`、`idempotency_key`、`event_type` 等必须大小写敏感。
- 展示名称可以在查询层另做 locale-aware 搜索，不改变身份键列排序规则。
- 不在 key 入库时自动 trim、lowercase 或 Unicode 折叠；规范化由 API 契约明确执行。
- Provider conformance 必须覆盖 `Key`、`key`、全角/组合字符等边界，确认两个数据库返回一致冲突结果。

### 3.4 JSON 与摘要

PostgreSQL `jsonb` 和 MySQL `json` 都会以各自内部格式保存并验证 JSON，但二者的键顺序、数值显示和查询语义不作为业务正确性基础：[PostgreSQL JSON](https://www.postgresql.org/docs/current/datatype-json.html)、[MySQL JSON](https://dev.mysql.com/doc/refman/8.4/en/json.html)。

- `request_hash`、`spec_digest`、`state_digest` 在 Rust 层基于同一 canonical JSON 编码计算。
- 数据库不通过 `jsonb =`、JSON path 或序列化文本判断幂等。
- v1 不为 JSON path 建业务关键索引；需要索引的状态、时间、ID 和 key 必须是一等列。
- JSON 字段设置大小上限并在写入前校验，不把大型代码、日志或二进制内容嵌入 JSON。
- schema version 是独立列或 envelope 必需字段，迁移不得猜测历史 JSON 结构。

### 3.5 NULL 与唯一约束

PostgreSQL 普通 UNIQUE 默认允许多个 NULL；MySQL nullable UNIQUE 列同样允许多个 NULL。`wait_subscriptions.active_slot` 利用共同语义实现“一个 open 等价 Wait，多个历史终态 Wait”。PostgreSQL 不使用专属 partial unique index 或 `NULLS NOT DISTINCT`，尽管官方支持相关能力：[PostgreSQL UNIQUE 索引](https://www.postgresql.org/docs/current/indexes-unique.html)。

任何逻辑上要求“NULL 也只能一个”的约束不得依赖默认 UNIQUE 行为；应改成非空 sentinel/slot、独立守卫表或领域事务。

## 4. Schema 与角色边界

### 4.1 Schema

- PostgreSQL 默认使用独立 schema `agent_loom`，连接设置固定 `search_path` 或始终限定 schema。
- MySQL 使用独立 database，禁止与其他应用共享同名表。
- schema/database 名称是部署配置，不进入领域 ID 或事件 payload。
- Provider 启动时读取 migration history，发现缺失、dirty、未知未来 migration 时返回 `MIGRATION_REQUIRED`。

### 4.2 数据库角色

| 角色 | 权限 |
| --- | --- |
| migration owner | DDL、索引、约束、授权；不作为 Runtime 连接 |
| runtime writer | 必需表的 SELECT/INSERT/UPDATE；无 DDL、DROP、TRUNCATE |
| runtime reader | 授权查询视图或表 SELECT；不参与权威命令 |
| archive/privacy operator | 受审计的归档、擦除和密钥销毁操作 |

不可变表收紧权限：

- `events`、`checkpoints`、`task_attempts`、`tool_execution_attempts`、`agent_event_receipts` 对 runtime writer 只开放 SELECT/INSERT。
- `artifact_refs` 原则上只 INSERT/SELECT；业务状态变化通过 Event/Gate Artifact 表达。
- 不使用触发器阻止 UPDATE，因为触发器会形成 Provider 专属隐藏语义；授权和 Provider API 共同约束。

## 5. Migration 元数据

### 5.1 `schema_migrations`

```text
logical_id, provider_kind, physical_checksum, logical_model_version,
state, started_at, applied_at, runner_version, details_json
```

约束：

- 主键：`logical_id`；
- `provider_kind` 必须与当前 Provider 相符；
- `state`：`applying/applied/failed`；
- 已 applied migration 的文件 checksum 变化必须阻止启动，不能静默重算；
- PostgreSQL/MySQL 同一变更共享 `logical_id` 和 `logical_model_version`，允许 physical checksum 不同。

### 5.2 执行锁

生产部署必须保证同一数据库只有一个 migration runner。具体实现可以使用部署平台单实例门、PostgreSQL advisory lock 或 MySQL named lock，但这些锁不进入 Runtime 正确性路径。

MySQL DDL 可能隐式提交，不能假设整个 migration 文件事务化。因此每个 migration：

1. 预检当前 schema 与 capability；
2. 写入/确认 `applying` 记录；
3. 以可重复检测的独立 DDL step 执行；
4. 校验 Information Schema/catalog 实际结果；
5. 标记 `applied`；
6. 失败后停止后续 migration，由同一版本 runner 做幂等恢复或人工处置。

PostgreSQL 即使允许事务 DDL，也采用相同 step journal；`CREATE INDEX CONCURRENTLY` 等不能放入普通事务的步骤单独执行。

### 5.3 禁止自动 schema sync

- Runtime 启动时不得自动创建、删除或修改业务表。
- 禁止 ORM 根据当前 struct 自动 diff 生产 schema。
- 测试环境可从 migration 目录创建空库，但仍运行正式 migration runner。
- 回滚优先通过后续 forward migration 修复；不可逆 contract migration 不提供虚假的 down SQL。

## 6. 表清单与归属

| 表 | 类型 | 聚合/用途 | 初始批次 |
| --- | --- | --- | --- |
| `schema_migrations` | mutable journal | schema 版本 | `0000` |
| `tenants` | mutable projection | tenant 边界 | `0001` |
| `workflow_definitions` | mutable identity | Workflow 身份 | `0001` |
| `workflow_definition_versions` | immutable version | Workflow 版本 | `0001` |
| `agent_definitions` | mutable identity | Agent 身份 | `0001` |
| `agent_definition_versions` | immutable version | Agent 版本 | `0001` |
| `agent_endpoints` | mutable config | 远程 Endpoint | `0002` |
| `runs` | mutable aggregate root | 生命周期/控制 | `0003` |
| `events` | immutable append | 权威事件序列 | `0003` |
| `command_receipts` | append/expiry | 命令幂等 | `0003` |
| `stage_executions` | mutable projection | 业务阶段 | `0004` |
| `tasks` | mutable queue | Worker 工作 | `0004` |
| `task_attempts` | immutable append | Lease/执行审计 | `0004` |
| `checkpoints` | immutable append | 恢复快照 | `0004` |
| `wait_subscriptions` | mutable one-shot | 等待条件与恢复计划 | `0005` + `0007` |
| `artifact_refs` | immutable append | 交付物引用 | `0005` |
| `tool_executions` | mutable projection | Tool 副作用与重试时间 | `0006` + `0008` |
| `tool_execution_attempts` | immutable append | Tool 请求审计 | `0006` |
| `agent_executions` | mutable projection | 远程 Agent 映射 | `0006` |
| `agent_event_receipts` | immutable guard | 远程 Event 去重 | `0006` |
| `outbox_messages` | mutable delivery | 可选消息投递 | `0009` optional |

`agent_event_receipts` 是 `append_agent_events` 的必要去重守卫。只把 vendor event ID 放进 JSON 无法建立跨 Provider 的可靠唯一约束。

## 7. 定义与配置表

### 7.1 `tenants`

必需列：

```text
tenant_id Id PK
tenant_key Key NOT NULL
status Status NOT NULL
policy_json Json NOT NULL
created_at Instant NOT NULL
updated_at Instant NOT NULL
```

约束与索引：

- `uq_tenants__key (tenant_key)`；
- `ck_tenants__status`：`active/suspended/deleting`；
- `ix_tenants__status (status, updated_at, tenant_id)`。

### 7.2 Workflow 定义

`workflow_definitions`：

- PK `workflow_id`；
- UNIQUE `(tenant_id, workflow_id)` 供复合 FK；
- UNIQUE `(tenant_id, workflow_key)`；
- FK `tenant_id → tenants`，删除 RESTRICT；
- `latest_version` 是可空缓存，不作为 Run 引用依据。

`workflow_definition_versions`：

- PK `workflow_version_id`；
- UNIQUE `(tenant_id, workflow_version_id)`；
- UNIQUE `(tenant_id, workflow_id, version)`；
- FK `(tenant_id, workflow_id) → workflow_definitions`；
- lifecycle CHECK：`draft/published/retired`；
- `version >= 1`，`spec_digest` 固定 32 字节；
- published 后内容不可修改由 Provider API/权限保证，不使用 trigger。

### 7.3 Agent 定义

`agent_definitions` 与 Workflow identity 表同型：

- PK `agent_id`；
- UNIQUE `(tenant_id, agent_id)`、`(tenant_id, agent_key)`；
- status CHECK：`active/archived`。

`agent_definition_versions`：

- PK `agent_version_id`；
- UNIQUE `(tenant_id, agent_version_id)`；
- UNIQUE `(tenant_id, agent_id, version)`；
- FK 指向 `(tenant_id, agent_id)`；
- published 版本不可变。

### 7.4 `agent_endpoints`

- PK `endpoint_id`；候选 UNIQUE `(tenant_id, endpoint_id)`；
- UNIQUE `(tenant_id, endpoint_key)`；
- FK tenant，删除 RESTRICT；
- `base_uri`、`credential_ref` 不进入普通日志；
- capability JSON 是发现缓存，不替代 AgentExecution snapshot；
- 索引 `(tenant_id, adapter_kind, status, endpoint_id)`。

## 8. Run、Event 与幂等核心

### 8.1 `runs`

字段按 [DOMAIN_MODEL.md](./DOMAIN_MODEL.md) 5.1 创建。关键物理约束：

```text
PK (run_id)
UNIQUE (tenant_id, run_id)
FK tenant_id → tenants
FK (tenant_id, workflow_version_id) → workflow_definition_versions nullable
FK (tenant_id, coordinator_agent_version_id) → agent_definition_versions nullable
FK (tenant_id, parent_run_id) → runs nullable
CHECK version >= 0
CHECK execution_generation >= 0
CHECK next_event_sequence >= 1
CHECK deadline IS NULL OR deadline >= created_at
```

终态列 CHECK：

```text
status IN ('completed','failed','cancelled','timed_out')
  ↔ terminal_event_id IS NOT NULL AND terminal_at IS NOT NULL
```

`terminal_event_id` 在 `events` 创建后增加 `(tenant_id, run_id, terminal_event_id)` 复合 FK。`current_checkpoint_id` 同理在 `checkpoints` 创建后增加同 Run 复合 FK；`parent_task_id` 在 `tasks` 创建后增加 tenant 复合 FK。循环 FK 的创建顺序不能靠禁用外键检查绕过。

索引：

```text
ix_runs__tenant_status_page
  (tenant_id, status, updated_at, run_id)
ix_runs__workflow_page
  (tenant_id, workflow_version_id, created_at, run_id)
ix_runs__parent
  (tenant_id, parent_run_id, created_at, run_id)
ix_runs__deadline
  (status, deadline, run_id)
```

deadline 索引允许 NULL；Scheduler 谓词必须显式 `deadline IS NOT NULL`。

### 8.2 `events`

关键约束：

```text
PK (event_id)
UNIQUE (tenant_id, event_id)
UNIQUE (tenant_id, run_id, event_id)
UNIQUE (tenant_id, run_id, sequence)
FK (tenant_id, run_id) → runs
CHECK sequence >= 1
```

索引：

```text
ix_events__correlation (tenant_id, correlation_id, recorded_at, event_id)
ix_events__type_time (tenant_id, event_type, recorded_at, event_id)
```

`UNIQUE (tenant_id, run_id, sequence)` 已覆盖权威 Event 分页，不额外创建重复普通索引；`UNIQUE (tenant_id, run_id, event_id)` 用于保证 terminal/consumed/created/local Event 指针属于同一 Run。Event sequence 由持有 Run 锁的事务读取并递增 `runs.next_event_sequence` 后分配，不使用全库 sequence 作为 Run 顺序。

v1 不分区。PostgreSQL 分区唯一约束和 MySQL 分区键规则不同；只有引入独立 `event_keys` 守卫并通过归档/分页对等测试后才允许分区。

### 8.3 `command_receipts`

```text
PK (receipt_id)
UNIQUE (tenant_id, receipt_id)
UNIQUE (tenant_id, scope, idempotency_key)
FK tenant_id → tenants
CHECK outcome_kind IN ('applied','duplicate','no_op','rejected','conflict','outcome_unknown')
```

索引：

- `(tenant_id, resource_type, resource_id, created_at, receipt_id)`；
- `(expires_at, receipt_id)` 供受控清理。

`resource_id` 是多态诊断引用，不建通用 FK。Receipt 必须与领域转换同事务提交；先插入守卫后发现重复时读取原 `request_hash/outcome`，不同 hash 返回稳定冲突。

## 9. Stage、Task 与 Checkpoint

### 9.1 `stage_executions`

```text
PK (stage_execution_id)
UNIQUE (tenant_id, stage_execution_id)
UNIQUE (tenant_id, run_id, stage_key, attempt)
FK (tenant_id, run_id) → runs
FK (tenant_id, parent_stage_execution_id) → stage_executions nullable
FK (tenant_id, agent_version_id) → agent_definition_versions nullable
CHECK version >= 0
CHECK attempt >= 1
```

状态 CHECK 覆盖 `planned/active/waiting_approval/rework_required/succeeded/failed/skipped/cancelled`。

索引：

- `(tenant_id, run_id, status, stage_key, attempt)`；
- `(tenant_id, assignee_kind, assignee_ref, status, updated_at, stage_execution_id)`。

### 9.2 `tasks`

关键约束：

```text
PK (task_id)
UNIQUE (tenant_id, task_id)
UNIQUE (tenant_id, run_id, logical_key, generation)
FK (tenant_id, run_id) → runs
FK (tenant_id, stage_execution_id) → stage_executions nullable
CHECK generation >= 0
CHECK attempt >= 0 AND max_attempts >= 1 AND attempt <= max_attempts
```

Lease 一致性 CHECK：

```text
status = 'leased'
  ↔ lease_owner IS NOT NULL
     AND lease_token IS NOT NULL
     AND lease_expires_at IS NOT NULL
```

终态/非 Lease 状态必须清空热行中的 owner/token/expiry。`lease_token` 保存随机原值供完成命令验证，日志和 `task_attempts` 只保存 digest。

索引：

```text
ix_tasks__claim_global
  (status, available_at, priority DESC, task_id)
ix_tasks__claim_tenant
  (tenant_id, status, available_at, priority DESC, task_id)
ix_tasks__lease_reclaim
  (status, lease_expires_at, task_id)
ix_tasks__run_page
  (tenant_id, run_id, status, created_at, task_id)
```

MySQL 8.x 与 PostgreSQL 均支持降序 B-tree key；若某受支持版本的 planner 无法利用混合方向，允许使用全升序物理索引并在小候选窗口排序，但领域领取顺序保持 `priority DESC, available_at ASC, task_id ASC`。

### 9.3 `task_attempts`

- PK `task_attempt_id`；UNIQUE `(tenant_id, task_attempt_id)`；
- UNIQUE `(tenant_id, task_id, attempt)`；
- FK `(tenant_id, task_id) → tasks`、`(tenant_id, run_id) → runs`；
- `attempt >= 1`；
- 领取时 insert，结束时以 `finished_at IS NULL` 和 Lease 证明条件 finalize 一次；finalize 后不可修改，禁止 delete；
- 查询索引 `(tenant_id, run_id, task_id, attempt)`。

### 9.4 `checkpoints`

- PK `checkpoint_id`；候选 UNIQUE `(tenant_id, checkpoint_id)`；
- UNIQUE `(tenant_id, run_id, checkpoint_id)` 供 Run 当前指针证明同 Run 归属；
- UNIQUE `(tenant_id, run_id, sequence)`；
- FK run、Workflow version、Agent version 和 created Event；
- `sequence >= 1`、`execution_generation >= 0`；
- 外部调用前 insert，结果返回后以 `request_finished_at IS NULL` 条件 finalize 一次；finalize 后不可修改，禁止 delete；
- 索引 `(tenant_id, run_id, created_at, checkpoint_id)`。

`runs.current_checkpoint_id` 使用 `(tenant_id, run_id, current_checkpoint_id)` 复合 FK，更新必须与新 Checkpoint 插入同事务，且领域条件要求新 sequence 大于旧 sequence。数据库 FK 保证同 Run 归属，不保证单调性。

## 10. Wait 与 Artifact

### 10.1 `wait_subscriptions`

关键约束：

```text
PK (wait_id)
UNIQUE (tenant_id, wait_id)
UNIQUE (
  tenant_id, run_id, wait_type, expected_event_type,
  match_key_hash, active_slot
)
FK run/stage/consumed_event/created_event（均带 tenant）
UNIQUE (tenant_id, resume_task_id)
```

slot CHECK：

```text
(status = 'open' AND active_slot = 1)
OR
(status IN ('consumed','expired','cancelled') AND active_slot IS NULL)
```

索引：

- `(tenant_id, status, expected_event_type, match_key_hash, wait_id)`；
- `(status, expires_at, wait_id)`；
- `(tenant_id, run_id, status, created_at, wait_id)`。

消费使用条件更新或 Run → Wait 行锁，唯一 slot 只防止重复 open，不替代单次消费事务。

`0007_wait_resume_plan` 为 Wait 增加 `resume_task_id`、logical key、kind、priority、max attempts、基础 input 和 deadline。迁移对既有 Wait 生成确定性的 reconcile 恢复计划，再收紧 NOT NULL/CHECK；新 Wait 必须在创建时写入真实流程恢复计划。

### 10.2 `artifact_refs`

- PK `artifact_id`；候选 UNIQUE `(tenant_id, artifact_id)`；
- UNIQUE `(tenant_id, run_id, logical_key, version)`；
- FK run、stage、task、created Event；
- `version >= 1`、`size_bytes >= 0`；
- digest 固定 32 bytes；
- immutable insert-only。

索引：

- `(tenant_id, run_id, stage_execution_id, kind, version, artifact_id)`；
- `(tenant_id, digest, artifact_id)`；
- `(tenant_id, run_id, logical_key, version)` 已由 UNIQUE 覆盖。

`source_artifact_refs_json` 由应用验证 tenant、存在性和确定 version。v1 不把可变长度血缘拆成关系表；如果需要 SQL 图查询，再以 expand migration 增加 `artifact_edges`，不能只在某个 Provider 上解析 JSON。

## 11. Tool 与 Agent 外部执行

### 11.1 `tool_executions`

```text
PK (tool_execution_id)
UNIQUE (tenant_id, tool_execution_id)
UNIQUE (tenant_id, tool_call_id)
UNIQUE (tenant_id, idempotency_scope, idempotency_key)
FK run/stage/task（均带 tenant）
CHECK attempt_count >= 0
```

状态 CHECK 覆盖 `planned/executing/retry_scheduled/succeeded/failed/outcome_unknown/reconciling/compensated/manual_review`。

索引：

- `(status, updated_at, tool_execution_id)` 供 stale execution 对账；
- `(tenant_id, run_id, status, updated_at, tool_execution_id)`；
- 非空 `external_ref` 只建查询索引，除非具体 Tool profile 能证明作用域唯一。

### 11.2 `tool_execution_attempts`

字段：

```text
tool_attempt_id, tenant_id, tool_execution_id, run_id, attempt,
request_started_at, request_finished_at, adapter_error_code,
retry_class, remote_request_id, external_ref, response_digest,
outcome, metrics_json
```

- PK `tool_attempt_id`；
- UNIQUE `(tenant_id, tool_execution_id, attempt)`；
- FK ToolExecution 与 Run；
- immutable insert-only；
- 原始响应不默认入库，只保存脱敏摘要或受控诊断 ArtifactRef。

### 11.3 `agent_executions`

关键约束：

```text
PK (agent_execution_id)
UNIQUE (tenant_id, agent_execution_id)
UNIQUE (tenant_id, endpoint_id, idempotency_key)
UNIQUE (tenant_id, endpoint_id, remote_run_ref)  # remote ref 非空时
FK run/stage/task/endpoint/agent_version（均带 tenant）
CHECK version >= 0 AND cursor_version >= 0
```

PostgreSQL/MySQL 普通 UNIQUE 都允许多个 NULL，因此 nullable `remote_run_ref` 可以直接使用复合 UNIQUE；空字符串必须在 Adapter 边界拒绝。

索引：

- `(status, updated_at, agent_execution_id)` 供 stale submission/event sync；
- `(tenant_id, run_id, status, updated_at, agent_execution_id)`；
- `(tenant_id, endpoint_id, remote_session_ref, updated_at, agent_execution_id)`。

### 11.4 `agent_event_receipts`

字段：

```text
agent_event_receipt_id, tenant_id, agent_execution_id, run_id,
dedupe_key, source_event_id, source_sequence, source_cursor,
event_kind, raw_digest, local_event_id, recorded_at
```

约束：

```text
PK (agent_event_receipt_id)
UNIQUE (tenant_id, agent_execution_id, dedupe_key)
UNIQUE (tenant_id, agent_event_receipt_id)
UNIQUE (tenant_id, local_event_id)  # local_event_id nullable for transient/ignored
FK AgentExecution/Run
FK (tenant_id, run_id, local_event_id) → events nullable
CHECK source_sequence IS NULL OR source_sequence >= 0
```

`dedupe_key` 是 Adapter 根据 [ADAPTER_CONTRACT.md](./ADAPTER_CONTRACT.md) 生成的固定长度 digest，不直接索引任意 vendor ID。`append_agent_events` 持有 Run/AgentExecution 锁时先查 receipt；对新权威事件预生成本地 Event ID，按立即 FK 可满足的顺序插入 Event 再插入 receipt，随后写 Artifact/Wait/Task 并推进 cursor。所有写入仍在同一事务，任一步失败整体回滚。UNIQUE 冲突时回滚并读取既有 receipt，确认 raw digest 相同后视为 duplicate；digest 不同返回协议冲突并停止推进。

索引：

- `(tenant_id, agent_execution_id, source_sequence, agent_event_receipt_id)`；
- `(tenant_id, run_id, recorded_at, agent_event_receipt_id)`。

## 12. 可选 Outbox

`outbox_messages` 只在启用消息分发能力时创建：

```text
PK (outbox_id)
UNIQUE (tenant_id, outbox_id)
UNIQUE (tenant_id, event_id, topic)
FK event/tenant
CHECK attempt >= 0
```

索引：

- `(status, available_at, outbox_id)` 发布领取；
- `(status, lease_expires_at, outbox_id)` Lease 回收；
- `(tenant_id, partition_key, created_at, outbox_id)` 诊断查询。

Outbox 的领取复用 Task 的短事务和 Lease 语义，但不修改 Run 状态。没有 Outbox 时，DurableFollowUp 仍由权威任务表和周期扫描保证。

## 13. 外键策略与循环引用

### 13.1 通用规则

- 所有业务 FK 带 `tenant_id`，引用父表 `(tenant_id, id)` 候选唯一键。
- 每个含 tenant 且使用单列 ID 主键的业务表都创建 `(tenant_id, id)` 候选 UNIQUE；即使当前没有子表引用也保持规则一致。
- 默认 `ON DELETE RESTRICT/NO ACTION`，不使用 CASCADE 删除审计链。
- 普通 Runtime 不删除 Tenant/Run；归档和隐私流程显式按依赖顺序处理。
- FK 不设 DEFERRABLE，因为 MySQL 不提供等价语义；事务写入顺序必须立即满足约束。
- 多态引用（CommandReceipt resource、Event causation）不伪造通用 FK。
- 跨行/跨表状态不变量由 DurableStore 事务保证，不写触发器。

### 13.2 循环引用安装顺序

首建 `runs` 时以下列可存在但暂不添加 FK：

```text
parent_task_id
current_checkpoint_id
terminal_event_id
```

创建 Task、Checkpoint、Event 及其候选 UNIQUE 后，分别通过 migration step 增加复合 FK。其中 current Checkpoint 和 terminal Event 使用 `(tenant_id, run_id, referenced_id)`，直接防止跨 Run 指针。添加前运行 orphan preflight；空库初始迁移也遵守相同步骤，以保证未来演进脚本可复用。

不得在 MySQL 通过长期关闭 `foreign_key_checks` 作为正常迁移策略。若维护窗口内临时关闭，必须有离线校验和显式审批，且不用于在线 expand migration。

## 14. 事务隔离、锁顺序与时间

### 14.1 统一隔离级别

Provider 权威写事务统一使用 `READ COMMITTED`：

- PostgreSQL 默认即为 READ COMMITTED，但连接仍在测试中断言；
- MySQL/InnoDB 默认通常为 REPEATABLE READ，Provider 连接池必须显式配置 READ COMMITTED；官方文档说明两种级别的 snapshot 行为不同：[MySQL Transaction Isolation](https://dev.mysql.com/doc/refman/8.4/en/set-transaction.html)、[PostgreSQL Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html)。

正确性来自显式 `FOR UPDATE`、版本/generation/Lease 条件和唯一约束，不来自更强默认隔离。未来使用 SERIALIZABLE 只能作为 Provider 内部实验，并映射序列化冲突，不能改变领域结果。

### 14.2 锁顺序

所有聚合事务遵循：

```text
CommandReceipt guard
  → Tenant（命令依赖 active 状态时）
  → Run
  → StageExecution
  → Task / WaitSubscription
  → ToolExecution / AgentExecution
  → ArtifactRef / Checkpoint / Event / Outbox append
```

同层多行按原始 ID 字节升序锁定。任何新事务必须在设计评审中列出锁集合；禁止通过“偶尔死锁后重试”掩盖反向锁序。

CommandReceipt 首次 INSERT 的唯一键竞争可以先于 Run 锁。若冲突，事务读取既有 Receipt 并直接返回，不继续锁 Run。插入成功后才进入固定聚合锁序。

### 14.3 数据库权威时间

- Lease、deadline、Wait expires、retry available_at 的裁决使用同一事务读取的数据库当前时间。
- PostgreSQL 使用 transaction timestamp 语义；MySQL 使用 `CURRENT_TIMESTAMP(6)`，Provider 将其读取为 `db_now` 并在该事务复用。
- 不用 Worker 本机时间判断 Lease 是否有效。
- `occurred_at` 可以来自外部生产者，但状态裁决使用 `recorded_at/db_now`。
- 测试通过可注入的 Clock 生成命令时间，但生产 Provider 最终条件仍比较数据库时间。

## 15. Task 领取与 `SKIP LOCKED`

### 15.1 为什么不能直接全局锁 Task

状态机规定 Run 先于 Task 加锁。以下常见队列 SQL 会先锁 Task，再锁 Run，可能与 Complete/Pause/Cancel 的 `Run → Task` 事务形成反向死锁，因此禁止作为 v1 领取实现：

```sql
SELECT task_id
FROM tasks
WHERE status = 'queued'
ORDER BY priority DESC, available_at, task_id
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

### 15.2 标准两段式领取

第一段是无锁、可丢弃的候选扫描：

```sql
SELECT t.tenant_id, t.run_id, t.task_id
FROM tasks t
JOIN runs r
  ON r.tenant_id = t.tenant_id AND r.run_id = t.run_id
JOIN tenants n
  ON n.tenant_id = t.tenant_id
WHERE t.status = 'queued'
  AND t.available_at <= :scan_now
  AND n.status = 'active'
  AND r.status IN ('queued', 'running')
  AND t.generation = r.execution_generation
  AND (r.deadline IS NULL OR r.deadline > :scan_now)
ORDER BY t.priority DESC, t.available_at, t.task_id
LIMIT :candidate_window;
```

候选不代表领取成功。Worker 对候选逐个调用权威 `claim_task`：

```text
BEGIN READ COMMITTED
  INSERT CommandReceipt guard
  SELECT Tenant by tenant_id FOR SHARE
  SELECT Run by (tenant_id, run_id) FOR UPDATE [SKIP LOCKED/NOWAIT]
  SELECT Task by (tenant_id, task_id) FOR UPDATE [SKIP LOCKED/NOWAIT]
  read db_now
  revalidate tenant/run/task/generation/deadline/available_at
  UPDATE Task → leased, token, owner, expiry, attempt
  INSERT TaskAttempt
  append Event and project Run if required
COMMIT
```

- Tenant suspend 使用同一行的排他锁；因此 suspend 提交后，新的 claim 不能越过 tenant 控制门。
- `SKIP LOCKED`/`NOWAIT` 只让竞争失败更快；没有该优化时短暂等待后重验也正确。
- Worker 不跨事务保留锁或候选资格。
- claim 每次返回一个 Task，避免长批事务和不公平占用。
- 候选窗口全部冲突时重新 keyset 扫描并加入有界 jitter。
- 同一 Run 的多个 fan-out Task 可以被连续短事务领取，不要求长期串行执行。

### 15.3 CAS 更新条件

即使已经行锁，UPDATE 仍带防御条件：

```text
task_id + tenant_id
status = queued
generation = locked_run.execution_generation
available_at <= db_now
run non-paused/non-terminal and deadline valid
```

影响行数不是 1 时回滚或返回稳定 conflict。Provider 不把“SELECT 看见可用”当作最终证明。

## 16. 关键事务 SQL 形状

### 16.1 Event sequence 分配

持有 Run 行锁后：

```text
sequence = runs.next_event_sequence
UPDATE runs
  SET next_event_sequence = sequence + 1,
      version = version + :transition_increment
  WHERE tenant_id = ? AND run_id = ? AND version = ?
INSERT events(..., sequence, ...)
```

PostgreSQL 可以使用 `UPDATE ... RETURNING`，MySQL 可以先读取锁定行再 UPDATE；两者必须产生相同 sequence。一次事务追加多个 Event 时预留连续区间，并按固定顺序插入。

### 16.2 Wait 消费

```text
INSERT/resolve CommandReceipt
lock Run
lock Wait by exact indexed match
validate status=open, active_slot=1, expires_at > db_now
UPDATE Wait SET status=consumed, active_slot=NULL, consumed_by_event_id=...
INSERT Event
INSERT recovery Task blocked by pause generation when needed
INSERT Checkpoint
UPDATE Run projection/version
COMMIT
```

两个消费者竞争时，Wait 行锁和条件更新决定唯一获胜者；UNIQUE active slot 防止重复创建，不承担消费仲裁。

### 16.3 Complete 与 terminal command

Complete/Cancel/Timeout 都先锁 Run，并验证 `terminal_event_id IS NULL`、当前 status/version 和 generation。获胜事务：

1. 分配唯一 Event sequence；
2. 写 terminal Event；
3. 设置 `terminal_event_id/terminal_at/status`；
4. 完成或关闭 Stage/Task/Wait；
5. 写最终 Checkpoint/Artifact（如适用）；
6. 保存 Receipt 和 DurableFollowUp；
7. commit。

后到事务看到终态后返回 no-op/terminal receipt，不能再写第二个 terminal Event。FK + terminal CHECK 防御引用完整性，领域锁/CAS 保证竞争唯一性。

### 16.4 Agent event batch

```text
lock Run
lock AgentExecution
validate cursor_version
for normalized event in deterministic order:
  lookup AgentEventReceipt under aggregate locks
  if duplicate: verify raw_digest and reuse result
  if new and authoritative:
    insert local Event, then AgentEventReceipt referencing it
    insert derived Artifact / Wait / Task
  if new and transient/ignored:
    insert AgentEventReceipt with local_event_id = NULL
update AgentExecution cursor + cursor_version CAS
project Run and save Receipt/Checkpoint
commit
```

批次任一持久化失败必须整体回滚，cursor 不前进。Transient delta 可以不生成 local Event，但仍可按采样策略登记 receipt；影响状态的事件必须有 receipt。

## 17. 错误映射与事务重试

### 17.1 统一分类

| 数据库情况 | StoreError | RetryClass |
| --- | --- | --- |
| 唯一/FK/CHECK 违反 | `CONSTRAINT_VIOLATION` 或领域化冲突 | Never/ReloadState |
| CAS 影响 0 行 | `VERSION_CONFLICT`/`LEASE_LOST` | ReloadState |
| deadlock victim | `SERIALIZATION_CONFLICT` | Backoff |
| serialization failure | `SERIALIZATION_CONFLICT` | Backoff |
| lock timeout | `STORE_UNAVAILABLE` 或 conflict | Backoff |
| 连接在 commit 前明确失败 | `STORE_UNAVAILABLE` | Backoff |
| commit 响应丢失、结果未知 | `OUTCOME_UNKNOWN` | Reconcile |
| schema 版本不匹配 | `MIGRATION_REQUIRED` | Never |

Provider 内可以针对纯数据库领域事务有限重试 deadlock/serialization failure，但必须复用相同 CommandContext、ID、request hash 和 logical key。任何事务闭包中不得调用 Agent、LLM、HTTP、MCP 或 DevOps。

### 17.2 唯一冲突领域化

Provider 不能把所有 unique violation 都返回同一模糊错误。迁移中约束名称稳定，错误映射至少区分：

- CommandReceipt key 冲突 → 读取并判断 duplicate/key reused；
- Task logical key 冲突 → 已生成等价后续 Task；
- Wait active slot 冲突 → 已存在 open Wait；
- Event sequence 冲突 → 不变量告警；
- terminal ref/状态冲突 → terminal race 后重读；
- Agent event dedupe 冲突 → duplicate 或 payload mismatch。

原始 SQL、约束内容和 payload 不返回 API，只在脱敏 Provider span 中记录稳定 constraint tag。

## 18. Migration 批次

### `0000_migration_meta`

- 创建 `schema_migrations`；
- 写 provider kind、runner 和 baseline capability 结果；
- 不创建业务表。

### `0001_identity_definitions`

- tenants；
- workflow_definitions / versions；
- agent_definitions / versions；
- 复合候选唯一键与 tenant FK。

### `0002_agent_endpoints`

- agent_endpoints；
- endpoint key/status 索引；
- credential_ref 长度和非空约束。

### `0003_run_event_idempotency`

- runs，暂缓三个循环 FK；
- events；
- command_receipts；
- 增加 `runs.terminal_event_id → events` FK；
- 安装控制台、deadline、Event 分页和 receipt expiry 索引。

### `0004_stage_task_checkpoint`

- stage_executions；
- tasks、task_attempts；
- checkpoints；
- 增加 runs.parent_task/current_checkpoint FK；
- 安装 claim、reclaim、Stage/Task 查询索引。

### `0005_wait_artifact`

- wait_subscriptions + active slot UNIQUE/CHECK；
- artifact_refs + contract/source lineage 字段；
- 安装事件匹配、超时、digest 和 logical artifact 索引。

### `0006_external_executions`

- tool_executions、tool_execution_attempts；
- agent_executions、agent_event_receipts；
- stale execution、remote ref、event dedupe/cursor 索引。

### `0007_wait_resume_plan`

- expand Wait 恢复计划字段并为历史 Wait 生成确定性 reconcile 计划；
- 收紧 NOT NULL、Task kind、重试上限与 deadline CHECK；
- 增加 `(tenant_id, resume_task_id)` 唯一约束。

### `0008_tool_retry_schedule`

- 为 ToolExecution 增加数据库时间语义的 `retry_at`；
- 回填既有 retry 状态并增加状态/时间一致性 CHECK；
- 增加 due retry 扫描索引。

### `0009_optional_outbox`

- 仅启用消息分发 feature 的部署执行；
- 创建 outbox_messages 和领取/回收索引；
- 不改变 DurableStore 基础正确性或默认 capability。

### `0010_runtime_grants`

- 应用 runtime writer/reader 的最小权限；
- 收紧 append-only 表 UPDATE/DELETE；
- 校验 migration owner 与 Runtime credential 不同。

每个批次结束运行 schema introspection 和最小插入/回滚 smoke test。基础 Provider 的 schema readiness 以所有必需 migration（当前为 `0000` 至 `0008`）成功且 Provider 黑盒测试通过为准。`0009_optional_outbox` 不进入基础正确性门槛；生产部署还必须通过最小权限检查，权限可以由 `0010_runtime_grants` 或部署平台的等价 IaC 实现。

## 19. Expand → Backfill → Switch → Contract

### 19.1 Expand

- 只增加 nullable 列、带安全默认的新列、新表、新索引或宽松约束。
- 新状态值先扩展 CHECK，再部署会写入该状态的 Runtime。
- 新 reader 必须兼容旧行缺值；新 writer 在 feature flag 开启前可以双写。
- 大表新索引使用 Provider 安全路径：PostgreSQL `CREATE INDEX CONCURRENTLY`；MySQL 显式要求可接受的 `ALGORITHM/LOCK`，若服务器不能满足则失败而不是静默 COPY。

PostgreSQL 支持 concurrent index 以及 `NOT VALID`/`VALIDATE CONSTRAINT` 等降低在线验证阻塞的方式；MySQL Online DDL 支持 INSTANT/INPLACE/LOCK 选择，但具体操作能力不同，且最终仍可能需要 metadata lock：[PostgreSQL ALTER TABLE](https://www.postgresql.org/docs/current/sql-altertable.html)、[PostgreSQL CREATE INDEX](https://www.postgresql.org/docs/current/sql-createindex.html)、[MySQL Online DDL](https://dev.mysql.com/doc/refman/8.4/en/innodb-online-ddl.html)。迁移 runner 必须明确请求算法/锁级别并在不满足时停止。

### 19.2 Backfill

- 独立可恢复 job，不在 schema migration 长事务中扫描整表。
- 以主键/keyset 分块，每批短事务；保存 cursor、行数和错误摘要。
- UPDATE 带 `new_column IS NULL` 等幂等条件。
- 限流并监控复制延迟、锁等待、deadlock、undo/WAL 和磁盘空间。
- backfill 派生 digest 时使用当前应用 canonical codec 版本，并保存 codec version。
- 失败可以从 cursor 重启；不得依赖 OFFSET。

### 19.3 Switch

- 部署同时兼容旧/新 schema 的 Runtime；
- 启用双写并比较差异；
- reader 切换到新列/表；
- 停止旧 writer 前等待所有旧 Worker Lease、Checkpoint schema 和重试窗口结束；
- 运行 PostgreSQL/MySQL 相同校验查询与 E2E suite。

### 19.4 Contract

- 去除旧 reader/writer 后再增加 NOT NULL/严格 CHECK 或删除旧索引。
- 删除列/表必须独立发布并有备份、保留期和审计审批。
- 不在同一发布中“停止旧写 + 删除旧列”。
- MySQL 改类型/主键可能重建整表，必须先在等规模副本验证；PostgreSQL 可能获得高等级表锁，也必须设 lock timeout 和维护策略。
- contract 失败不回滚到会写旧 schema 的应用版本；通过 forward fix 恢复。

## 20. 索引验收与查询计划

每个 Provider 在固定规模 fixture 上保存关键 `EXPLAIN` 断言，但不绑定具体 cost 数字。至少验证：

1. Task 候选扫描命中 claim 索引，不全表扫描。
2. Run PK/tenant 复合锁定走唯一查找。
3. Lease reclaim、Wait expiry、Run deadline、stale Tool/Agent scan 走对应索引。
4. Event `sequence > cursor ORDER BY sequence LIMIT n` 走唯一索引范围扫描。
5. CommandReceipt lookup 走 `(tenant,scope,key)` 唯一索引。
6. Agent event dedupe 走 receipt 唯一索引。
7. E2E 场景 Artifact logical key/version 查询不扫描 JSON。

查询计划变化只触发性能告警，不自动改变领域结果。若统计信息异常导致扫描扩大，短事务 timeout 和候选窗口必须限制影响。

## 21. Provider 对等验收

### 21.1 Schema introspection

测试从系统 catalog/Information Schema 生成规范化 schema snapshot，比较：

- 逻辑表、列、可空性和逻辑类型；
- PK、UNIQUE、FK、CHECK 的逻辑意图；
- 索引前缀、排序用途和唯一性；
- append-only 表授权；
- applied logical migration IDs。

物理类型名、系统生成表达式文本、索引 access method 细节不要求逐字相同。

### 21.2 并发与故障测试

两端运行相同测试：

1. 100 Worker 并发 claim，同一 Task 仅一个 Lease。
2. 不同 Run 的 Task 可并行领取；同一 Run 短锁不会造成永久饥饿。
3. renew/reclaim/complete 竞争只有一个合法结果。
4. 100 次重复 Command 只生成一个 Receipt/状态 Event。
5. Event sequence 连续、唯一并可 keyset 补读。
6. 两个事件只消费一个 Wait；active slot 可在终态后创建新 attempt。
7. Complete/Cancel/Timeout 竞争只有一个 terminal Event。
8. Agent event batch 重放不重复本地 Event，payload mismatch 被拒绝。
9. commit 响应丢失后按 Receipt 对账，不创建新幂等键。
10. deadlock/serialization retry 不跨外部调用边界。
11. E2E-01、E2E-02、E2E-03 产生等价领域历史。

### 21.3 数据等价快照

测试将数据库行解码成领域对象后比较：

```text
Run/Stage/Task/Wait projections
Event type/sequence/correlation/causation
Checkpoint schema/generation/digest
Artifact logical key/version/digest/lineage
Receipt request hash/outcome
Tool/Agent normalized status and recovery action
durable follow-up cardinality
```

不比较物理 UUID 文本格式、JSON 键顺序、数据库内部时间显示、SQL 错误消息和 query plan cost。

## 22. 运维与可观测性

### 22.1 启动检查

Provider readiness 必须验证：

- vendor 与 capability；
- migration 无 dirty/unknown future state；
- MySQL 全部业务表为 InnoDB、连接时区 UTC，且 `STRICT_TRANS_TABLES`/等价严格模式与零日期拒绝策略生效；
- PostgreSQL search_path/权限符合配置；
- runtime role 无 DDL，append-only 表权限正确；
- 主库可写，权威命令未误连只读副本。

### 22.2 指标

至少暴露：

```text
migration_current_logical_id
migration_dirty
db_transaction_latency{operation,provider}
db_deadlock_retry_total{operation,provider}
db_lock_timeout_total{operation,provider}
task_claim_candidate_miss_total{reason}
task_claim_latency
lease_conflict_total
receipt_duplicate_total
event_sequence_conflict_total
agent_event_dedupe_total
schema_constraint_violation_total{constraint_tag}
```

### 22.3 备份与恢复

- 备份必须覆盖 schema_migrations 和全部权威业务表，不能只备份 Event。
- 恢复演练验证 FK/UNIQUE/CHECK、migration checksum 和 Artifact 外部引用可达性。
- Point-in-time restore 后，所有外部 Tool/Agent executing/submitting 记录先进入 reconcile 扫描，不能直接重放。
- 恢复到旧时间点可能与外部系统现实不一致，发布/支付等副作用必须查询结果或人工确认。

## 23. Definition of Done

迁移设计只有同时满足以下条件才可进入编码：

1. 两个 Provider 的所有逻辑 migration ID、表和约束意图一一对应。
2. 所有 tenant 业务引用使用复合 tenant FK 或有明确多态例外。
3. Run 单终态、Event sequence、Task logical key、Wait active slot、CommandReceipt 和 Agent event dedupe 有数据库防御约束。
4. Task claim 遵循 Run → Task 锁序，`SKIP LOCKED` 不承担正确性。
5. MySQL 显式使用 InnoDB、UTC、strict mode 和 READ COMMITTED。
6. JSON canonical digest、ID codec 和时间 codec 在应用层共享。
7. 初始 migration 不依赖分区、触发器、数据库 enum 或 JSON path 索引。
8. expand/backfill/switch/contract 能支持至少一个真实字段演进演练。
9. Schema introspection、Provider conformance 和三个 E2E 场景在两端通过。
10. Runtime 凭据不能执行 DDL 或修改 append-only 审计表。

## 24. 后续实现产物

本文冻结后进入可执行工程产物：

1. 初始化 Rust workspace：`domain`、`durable-store`、`store-postgres`、`store-mysql`、`adapter-core`。
2. 定义共享 ID、Instant、Digest、Status 和 canonical JSON codec。
3. 为 `0000`—`0008` 生成两套 migration 文件和 schema snapshot 测试。
4. 建立 `durable-store` conformance harness 与数据库容器测试矩阵。
5. 实现最小 `create_run → claim_task → complete_task → event query` 垂直链路。
6. 接入 `workflow.delivery.v1` fixture 和 Mock Agent/DevOps Server，逐步跑通三条 E2E。

迁移 SQL 在生成后必须经过真实 PostgreSQL/MySQL 实例执行和并发测试；仅通过静态 SQL parser 或单一数据库测试不构成完成。
