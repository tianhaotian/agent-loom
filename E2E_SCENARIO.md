# Agent Loom 业产研交付端到端场景契约

## 1. 目的与边界

本文把 Agent Loom 的领域模型、状态机、存储契约和 Adapter 契约映射到首个可运行场景“业产研交付”。它同时是：

- `workflow.delivery.v1` 的业务语义基线；
- Phase 1 Mock Agent Server 垂直链路的验收规范；
- PostgreSQL 与 MySQL Provider 对等测试的场景输入；
- 后续 API、Worker、质量门禁和 DevOps Adapter 的共同样例。

本文不规定具体原型、代码托管、测试或 CI/CD 产品，也不把流程固化成不可扩展 DAG。Workflow Definition 只提供必需阶段、产物契约和默认门禁；运行时可以创建动态 Task、返工阶段和 Handoff，但不得绕过状态机或降低必需门禁。

本文依赖：

- [REQUIREMENT.md](./REQUIREMENT.md)：产品范围与功能需求；
- [STATE_MACHINE.md](./STATE_MACHINE.md)：状态、事务、幂等与竞争裁决；
- [DOMAIN_MODEL.md](./DOMAIN_MODEL.md)：领域实体、约束与 ArtifactRef；
- [STORE_CONTRACT.md](./STORE_CONTRACT.md)：原子操作和可靠后续动作；
- [ADAPTER_CONTRACT.md](./ADAPTER_CONTRACT.md)：Agent Server、Tool 与 DevOps 适配语义。

若本文中的业务示例与上述底层契约冲突，底层状态与事务契约优先；若底层契约无法表达本文的必需场景，则必须先升级底层契约版本，不能在场景实现中旁路。

## 2. 场景目标与参与者

### 2.1 业务目标

给定一个业务诉求，系统应协同人和多个专业 Agent，形成可追踪、可评审、可返工并可部署的完整交付链路：

```text
需求输入与澄清
  → 原型设计与 PRD
  → 技术方案与任务规划
  → 编码执行
  → 自测
  → 集成测试
  → DevOps 部署
  → 交付关闭
```

“完成”不是 Agent 返回一段文本，而是所有必需 Stage 成功、Artifact Contract 满足、质量门禁通过、部署结果已验证，且最终事实在同一 Run 的事件链中可审计。

### 2.2 参与者

| 参与者 | 默认职责 | 系统身份 |
| --- | --- | --- |
| 业务提出人 | 提交目标、约束与业务验收 | human actor |
| 产品负责人 | 澄清需求、确认 PRD 和范围变更 | human 或 product agent |
| 设计角色 | 产出原型并处理体验反馈 | human 或 design agent |
| 技术负责人 | 技术方案、风险和任务规划 | human 或 architecture agent |
| 开发角色 | 编码、自测与修复 | coding agent 或 human |
| 测试角色 | 集成测试、缺陷判定和复验 | test agent 或 human |
| 发布审批人 | 授权受保护环境部署 | human actor |
| DevOps 系统 | 构建、部署、健康验证和回滚 | Tool Adapter |
| 协调 Agent | 规划 Stage/Task、Handoff 和返工 | Agent Server Adapter |
| Agent Loom Runtime | 状态、事务、调度、等待和审计 | authoritative runtime |

同一角色可以由人或 Agent 承担，但 Workflow 必须保存 `assignee_kind`、`assignee_ref` 和使用的 Agent Definition 版本。权限门禁按动作风险决定，不能因为执行者是 Agent 而自动放宽。

## 3. Workflow Definition 基线

### 3.1 标识与版本

```text
workflow_key: delivery
workflow_version: 1
contract_version: delivery-contract/v1
required_stages:
  - requirements
  - product_design
  - technical_design
  - implementation
  - self_test
  - integration_test
  - deployment
  - delivery_closure
```

创建 Run 时固定 `workflow_version_id`。定义升级不改变正在运行的 Run；需要升级时创建迁移命令，保存迁移前 checkpoint、兼容性结果和迁移事件。

### 3.2 阶段依赖

| Stage key | 默认前置条件 | 可并行内容 | 默认后继 |
| --- | --- | --- | --- |
| `requirements` | Run 已创建且输入契约有效 | 资料检索、干系人问题收集 | `product_design` |
| `product_design` | RequirementSpec 已确认 | 原型与 PRD 可并行迭代 | `technical_design` |
| `technical_design` | PRD 基线和验收追踪已确认 | 架构、数据、API、测试策略 | `implementation` |
| `implementation` | 技术方案和实现计划通过门禁 | 按模块 fan-out 编码 | `self_test` |
| `self_test` | 代码变更可构建 | 单元、静态检查、安全扫描 | `integration_test` |
| `integration_test` | 自测通过且测试环境就绪 | 接口、端到端、兼容性测试 | `deployment` |
| `deployment` | 发布清单、审批和制品齐备 | 部署后多项健康检查 | `delivery_closure` |
| `delivery_closure` | 目标环境部署验证成功 | 文档归档、通知、指标汇总 | Run `completed` |

这些依赖是阶段激活条件，不代表用内存 DAG 调度。每次激活都通过 DurableStore 事务检查输入 Artifact、创建 StageExecution/Task 并追加事件。

### 3.3 StageExecution 规则

- 一个 Stage 可包含多个有限 Task；Task 全部成功不等于 Stage 成功。
- Stage 只有在输出 Artifact Contract 和质量门禁均满足后才进入 `succeeded`。
- 返工创建新的 attempt，不覆盖原 StageExecution 或 ArtifactRef。
- 动态 Stage 必须设置 `generated_by_event_id`，并声明它补充或阻塞哪个必需 Stage。
- 跳过可选 Stage 必须记录策略依据；上述八个必需 Stage 不允许静默跳过。
- 后继阶段只消费已确认的 Artifact version，不引用“最新版本”这种漂移指针。

## 4. 统一 Artifact Contract

### 4.1 ArtifactRef 公共信封

所有交付物通过不可变 `ArtifactRef` 登记：

```json
{
  "logical_key": "requirements/spec",
  "kind": "requirement_spec",
  "contract_version": "requirement-spec/v1",
  "version": 3,
  "uri": "artifact://tenant/run/requirements/spec/3",
  "digest": "sha256:...",
  "media_type": "application/json",
  "produced_by": {
    "actor_kind": "agent",
    "actor_ref": "product-agent:v2",
    "task_id": "task_..."
  },
  "source_artifacts": ["artifact_..."],
  "metadata": {
    "schema_valid": true,
    "classification": "internal"
  }
}
```

公共约束：

1. `logical_key` 在同一 Run 内稳定，修订只递增 version。
2. `digest` 验证内容完整性；不能把 URI 或更新时间当作内容摘要。
3. 产物必须关联产生它的 Task、Event 和 StageExecution。
4. `source_artifacts` 形成可查询的数据血缘；质量门禁只能消费明确版本。
5. 结构化产物必须通过对应 JSON Schema；非结构化文件至少有媒体类型、大小、摘要和受控 URI。
6. 大文件、设计稿、代码和制品保存在专用系统；Runtime 保存引用和可验证元数据。
7. 密钥、访问令牌和用户敏感数据不得写入普通 Artifact metadata。

### 4.2 Artifact 状态

ArtifactRef 本身不可变，业务可用性通过关联事实表达：

```text
produced → validated → approved → superseded
                  └──→ rejected
```

- `produced`：内容已登记，不代表满足阶段契约。
- `validated`：schema、引用完整性和自动质量检查通过。
- `approved`：需要人工门禁时，已绑定唯一审批决定。
- `superseded`：被同一 logical key 的新版本替代，但历史仍可审计。
- `rejected`：门禁失败；不得作为后继阶段的确认输入。

状态事实由 Event/审批记录表达，不原地修改 Artifact 内容。

## 5. 需求等价模型

### 5.1 目标

业务输入、访谈记录、PRD、用户故事、缺陷单和变更请求表达方式不同，但后续阶段需要一个稳定的需求身份与追踪模型。系统使用 `RequirementSpec` 作为规范化等价模型；它不替代原文，而是引用原始资料并提供机器可验证的交付语义。

### 5.2 RequirementSpec

```json
{
  "contract_version": "requirement-spec/v1",
  "requirement_set_id": "reqset_checkout",
  "revision": 4,
  "intent": "用户可使用企业账户完成月结支付",
  "scope": {
    "in": ["企业账户选择", "额度校验", "月结订单创建"],
    "out": ["授信审批", "账单催收"]
  },
  "actors": ["enterprise_buyer", "finance_admin"],
  "items": [
    {
      "requirement_id": "REQ-001",
      "type": "functional",
      "statement": "...",
      "business_rules": ["BR-001"],
      "acceptance_criteria": ["AC-001", "AC-002"],
      "priority": "must",
      "source_refs": ["source://brief#payment"],
      "status": "confirmed"
    }
  ],
  "non_functional": [
    {
      "requirement_id": "NFR-001",
      "dimension": "availability",
      "measure": ">=99.9% monthly",
      "verification": "service_slo_report"
    }
  ],
  "open_questions": [],
  "assumptions": [],
  "constraints": [],
  "source_digest": "sha256:...",
  "normalized_digest": "sha256:..."
}
```

每个 `requirement_id` 一经确认即保持稳定。文字润色、排序变化或格式迁移不得分配新 ID；业务语义变化则创建新 revision 并记录条目级差异。

### 5.3 等价、变更与冲突判定

| 判定 | 条件 | 处理 |
| --- | --- | --- |
| `exact` | canonical JSON 和 normalized digest 相同 | 复用既有解析结果，不创建新需求 revision |
| `equivalent` | 稳定 ID、范围、规则、验收条件和 NFR 均无语义变化 | 记录来源映射，可创建展示版本但不触发下游返工 |
| `compatible_extension` | 只增加不破坏已确认行为的可选项或说明 | 新 revision，运行影响分析，按策略决定局部返工 |
| `breaking_change` | 范围、规则、接口、数据、验收标准或 NFR 被修改/删除 | 新 revision，冻结受影响后继 Stage，创建 change approval |
| `conflict` | 两个有效来源对同一稳定条目给出互斥要求 | 创建 clarification Wait，不允许门禁通过 |
| `unknown` | 证据不足或模型置信不足 | 人工确认，不得自动当作 equivalent |

等价判定的硬约束：

- `source_digest` 只证明原文是否改变，不证明业务语义等价。
- `normalized_digest` 只用于完全规范化后的精确比较。
- LLM 可以提出条目映射和差异说明，但不能单独批准 `breaking_change` 或消解 `conflict`。
- 判定必须输出 `RequirementDiff` Artifact，列出 added、removed、modified、unchanged 条目和受影响的验收条件。
- 已进入 implementation 后的 breaking change 默认需要产品负责人和技术负责人审批。

### 5.4 端到端追踪矩阵

`TraceabilityMatrix` 至少维护：

```text
requirement_id
  → prd_section / prototype_node
  → design_decision / api_contract / data_change
  → implementation_task / change_ref
  → self_test_case
  → integration_test_case
  → release_id / deployment_environment
```

必需需求没有对应实现或测试，或者实现/测试无法回溯到需求时，相关阶段不得通过门禁。废弃需求保留历史链路并标记终止 revision，不能物理删除追踪关系。

## 6. 各阶段产物与质量门禁

### 6.1 `requirements`：需求输入与澄清

输入：

- 原始需求、背景材料和来源引用；
- 业务提出人、目标、优先级和期望时间；
- 已知系统边界、合规和预算约束。

必需输出：

| logical key | kind | 最低内容 |
| --- | --- | --- |
| `requirements/source_manifest` | `source_manifest` | 来源、摘要、权限和采集时间 |
| `requirements/spec` | `requirement_spec` | 目标、范围、稳定条目、规则、NFR 和验收条件 |
| `requirements/clarification_log` | `clarification_log` | 问题、回答、决定人和 causation |
| `requirements/diff` | `requirement_diff` | 与上一 revision 的条目级差异；首版标记 initial |

门禁 `gate.requirements.confirmed`：

- 所有 must 条目有可验证验收条件；
- open question 中不存在阻塞项；
- 范围内/范围外明确；
- 冲突来源已消解；
- RequirementSpec schema 和唯一 ID 检查通过；
- 产品负责人或授权业务角色确认需求基线。

失败后进入 `rework_required` 或创建 clarification Wait；等待期间不占用 Worker。

### 6.2 `product_design`：原型与 PRD

输入：已确认 RequirementSpec 和 SourceManifest。

必需输出：

| logical key | kind | 最低内容 |
| --- | --- | --- |
| `product/prototype` | `prototype_ref` | 版本化 URI、关键页面/流程节点和摘要 |
| `product/prd` | `prd` | 场景、交互、规则、异常路径、埋点和发布范围 |
| `product/traceability` | `traceability_matrix` | requirement → PRD/prototype 映射 |
| `product/review_report` | `review_report` | 完整性、一致性、可访问性和未决风险 |

门禁 `gate.product_design.approved`：

- 每个 must requirement 至少映射到 PRD 段落；需要界面的条目映射到 prototype node；
- 主路径、空状态、异常路径和权限差异明确；
- PRD 与 RequirementSpec 没有未解释冲突；
- 评审发现的问题已关闭或形成带责任人的风险接受记录；
- 产品负责人批准确定版本。

原型和 PRD 可由不同 Task 并行产出，但阶段完成事务必须绑定二者的确定版本和共同追踪矩阵。

### 6.3 `technical_design`：技术方案与任务规划

输入：已批准的 RequirementSpec、PRD、PrototypeRef 和 TraceabilityMatrix。

必需输出：

| logical key | kind | 最低内容 |
| --- | --- | --- |
| `technical/solution` | `technical_solution` | 架构、模块、接口、数据、兼容性、安全和可观测性 |
| `technical/decisions` | `architecture_decision_log` | 方案选择、备选项和取舍 |
| `technical/implementation_plan` | `implementation_plan` | Task、依赖、负责人、验收和回滚边界 |
| `technical/test_strategy` | `test_strategy` | 单元、集成、回归、性能和安全验证范围 |
| `technical/risk_register` | `risk_register` | 风险、概率、影响、缓解和 owner |
| `technical/traceability` | `traceability_matrix` | requirement → design/task/test strategy |

门禁 `gate.technical_design.approved`：

- 所有 must requirement 有设计决策和实现 Task；
- 数据迁移、向后兼容、灰度与回滚策略明确；
- 非幂等副作用、权限边界和敏感数据路径已标识；
- NFR 有可执行验证方式；
- 高风险项有缓解方案和 owner；
- 技术负责人批准基线。

ImplementationPlan 可以动态生成 Task，但每个 Task 必须有限、可重试边界清晰，并引用对应 requirement/design ID。

### 6.4 `implementation`：编码执行

输入：已批准技术方案、ImplementationPlan 和确定代码基线。

必需输出：

| logical key | kind | 最低内容 |
| --- | --- | --- |
| `implementation/change_set` | `change_set` | repository、base revision、head revision/PR、提交摘要 |
| `implementation/build_artifact` | `build_artifact` | 制品 URI、digest、构建环境和来源 ChangeSet digest |
| `implementation/task_results` | `implementation_result_set` | 每个计划 Task 的状态和证据 |
| `implementation/traceability` | `traceability_matrix` | requirement/design/task → file/change ref |
| `implementation/deviation_log` | `deviation_log` | 与技术方案的偏差、原因和审批 |

门禁 `gate.implementation.ready_for_test`：

- 所有 must implementation Task 完成或被显式替代；
- change set 可定位且 head digest 固定；
- BuildArtifact 可验证并绑定当前 ChangeSet digest；
- 构建成功，禁止提交的密钥和高危静态检查为零；
- 代码变更可回溯到 requirement 和 design decision；
- 未审批的方案偏差为零。

Coding Agent 通过远程 Agent Server 工作时，每次执行对应持久化 AgentExecution。远程 `completed` 只证明 Agent 执行完成；Runtime 仍需校验 ChangeSet 和门禁。

### 6.5 `self_test`：研发自测

输入：固定 ChangeSet、TestStrategy 和 RequirementSpec。

必需输出：

| logical key | kind | 最低内容 |
| --- | --- | --- |
| `test/self_test_report` | `test_report` | 测试集合、通过/失败/跳过、日志引用和环境 |
| `test/coverage_report` | `coverage_report` | 适用模块的覆盖证据及例外说明 |
| `test/static_analysis` | `analysis_report` | lint、类型、安全和依赖检查 |
| `test/self_traceability` | `traceability_matrix` | acceptance criteria → self-test case/result |

门禁 `gate.self_test.passed`：

- 必需构建、单元测试和静态检查成功；
- must acceptance criteria 至少有自测或明确标记为 integration-only；
- blocker/critical 缺陷为零；
- 跳过项有风险接受和后续验证 owner；
- 报告对应当前 ChangeSet digest，不接受旧代码报告。

失败触发 implementation 返工 attempt；修复后必须生成新的 ChangeSet 和 TestReport version。

### 6.6 `integration_test`：集成与端到端测试

输入：自测通过的 ChangeSet、RequirementSpec、PRD 和 TestStrategy。

必需输出：

| logical key | kind | 最低内容 |
| --- | --- | --- |
| `test/integration_plan` | `test_plan` | 环境、数据、依赖、用例与入口/退出条件 |
| `test/integration_report` | `test_report` | 用例结果、日志、失败证据和环境版本 |
| `test/defect_report` | `defect_report` | 缺陷级别、复现、关联 requirement 和处置 |
| `test/integration_traceability` | `traceability_matrix` | acceptance criteria → integration case/result |

门禁 `gate.integration_test.passed`：

- must acceptance criteria 全部有最终验证结果；
- blocker/critical 缺陷为零；允许遗留缺陷有明确风险接受；
- 外部依赖和测试环境版本可复现；
- 核心 NFR 达到 RequirementSpec 阈值；
- TestReport 对应待发布 ChangeSet digest。

测试失败时不得直接修改当前报告为通过。系统创建缺陷 Artifact、implementation 返工 Stage attempt，并在修复自测后创建新的 integration_test attempt。

### 6.7 `deployment`：DevOps 部署

输入：集成测试通过的 ChangeSet、可验证制品和环境策略。

必需输出：

| logical key | kind | 最低内容 |
| --- | --- | --- |
| `release/manifest` | `release_manifest` | release ID、artifact digest、配置版本、目标环境和变更清单 |
| `release/approval` | `approval_record` | 环境、决定、actor、时间、请求摘要和幂等键 |
| `release/deployment_record` | `deployment_record` | 外部操作引用、状态、环境、release 和时间 |
| `release/health_report` | `health_report` | 探针、业务指标、错误率、验证窗口和结论 |
| `release/rollback_record` | `rollback_record` | 仅发生回滚时必需；关联原部署 ToolExecution |

门禁 `gate.deployment.verified`：

- ReleaseManifest 的制品 digest 与已测试 ChangeSet/BuildArtifact 一致；
- 生产或其他受保护环境审批已原子消费；
- DevOps Adapter 返回的外部 release 与目标环境匹配；
- 健康验证窗口结束且技术、业务探针通过；
- 部署处于确定终态，不是 accepted、stopping 或 outcome_unknown。

HTTP 200 或部署命令 accepted 不等于成功。状态查询、健康验证和必要的 rollback 都是独立、持久化的 ToolExecution/Task。

### 6.8 `delivery_closure`：交付关闭

输入：DeploymentRecord、HealthReport 和完整 TraceabilityMatrix。

必需输出：

| logical key | kind | 最低内容 |
| --- | --- | --- |
| `delivery/summary` | `delivery_summary` | 范围、release、完成项、遗留项和负责人 |
| `delivery/final_traceability` | `traceability_matrix` | requirement 到 release 的完整链路 |
| `delivery/audit_manifest` | `audit_manifest` | 关键 Event、Artifact、审批和 Execution 引用 |

门禁 `gate.delivery.complete`：

- 所有必需 StageExecution 已成功；
- must requirement 均可追踪到最终 release 和验证证据；
- 没有 open blocker、未消费的必需审批或 outcome_unknown 外部执行；
- 最终 Artifact version 已固定；
- Run completion 事务同时写入最终 Event、Checkpoint、ArtifactRef 和唯一 `terminal_event_id`。

## 7. 质量门禁通用模型

### 7.1 GateDefinition

```json
{
  "gate_key": "gate.integration_test.passed",
  "contract_version": "quality-gate/v1",
  "required_artifacts": [
    {"logical_key": "test/integration_report", "min_version": 1}
  ],
  "automated_checks": ["schema", "change_digest_match", "critical_defect_zero"],
  "approval_policy": null,
  "on_pass": "activate:deployment",
  "on_fail": "rework:implementation",
  "timeout_policy": "fail"
}
```

GateDefinition 属于 Workflow Definition version；运行中的 GateExecution 固定其 definition 和输入 Artifact versions。

### 7.2 GateEvaluation Artifact

```json
{
  "gate_key": "gate.integration_test.passed",
  "evaluation_id": "gateeval_...",
  "input_artifacts": ["artifact_version_ref..."],
  "checks": [
    {"key": "critical_defect_zero", "outcome": "pass", "evidence": "artifact_..."}
  ],
  "outcome": "pass",
  "evaluated_by": "quality-agent:v1",
  "evaluated_at": "database timestamp"
}
```

允许的 outcome：

- `pass`：所有必需检查满足；
- `fail_rework`：可修复，创建返工计划；
- `fail_terminal`：不可恢复或策略禁止继续；
- `approval_required`：自动检查完成，等待人工决定；
- `waived`：显式风险接受，必须有授权 actor、理由、范围和过期时间；
- `inconclusive`：证据不足，不能推进 Stage。

`waived` 只有在 GateDefinition 明确允许时才能推进；schema、Artifact 完整性、唯一终态、发布制品身份和生产审批等正确性/安全约束不可豁免。

### 7.3 门禁原子性

门禁通过的提交事务至少包含：

```text
lock Run
  → lock StageExecution
  → validate generation/version and fixed Artifact versions
  → append gate evaluation Artifact/Event
  → consume approval Wait（如有）
  → mark current Stage succeeded
  → create/activate next Stage and initial Task
  → save Checkpoint and update Run projection
  → commit durable follow-ups
```

重复评估使用稳定 idempotency key，必须返回首次确定结果。相同 key 对应不同 Artifact version 时返回 `IDEMPOTENCY_KEY_REUSED`。

### 7.4 返工规则

- `fail_rework` 生成 `ReworkPlan`，包含原因、目标 Stage、受影响 requirement、需失效的下游输入和重新验证范围。
- 返工不删除历史成功 Stage；新 attempt 通过 parent/causation 关联旧实例。
- Requirement breaking change 可能使多个下游 Stage 需要重新执行；影响分析结果必须由策略或授权人确认。
- 只受展示文本变化影响且判定 equivalent 时，不重跑代码和测试。
- 新 ChangeSet 产生后，所有绑定旧 digest 的 TestReport 和 ReleaseManifest 不再可用于新门禁。

## 8. 动态任务、Handoff 与并行执行

### 8.1 动态 Task 计划

协调 Agent 可以在 Stage 内生成 `DynamicTaskPlan`：

```json
{
  "plan_id": "plan_...",
  "stage_execution_id": "stage_...",
  "generation": 5,
  "tasks": [
    {
      "logical_key": "implementation/module-a",
      "kind": "agent_execution",
      "depends_on": [],
      "required_capabilities": ["workspace_ref", "artifact_output"],
      "input_artifacts": ["technical/solution@2"],
      "expected_outputs": ["change/module-a"]
    }
  ]
}
```

- Plan 是 checkpoint 的一部分，同时登记生成它的 Event。
- Task logical key 在同一 generation 内唯一。
- fan-out Task 可以并行领取；fan-in Task 只有在全部必需输入确定后才激活。
- 动态计划不能删除 Workflow 必需 Stage、取消人工门禁或扩大 Adapter 权限。
- Pause/Resume、返工或需求变更导致 generation 增加后，旧 generation 的迟到结果只记录执行事实，不得推进当前流程。

### 8.2 Handoff

Handoff 必须携带：

```text
source actor / target actor
goal and bounded instructions
fixed input Artifact versions
expected output contract
budget / deadline / tool policy
correlation_id / causation_id
```

禁止只传递一段无法追踪来源的对话摘要。最近成功用户消息可以作为恢复上下文，但业务阶段输入必须引用已确认 Artifact。

### 8.3 Adapter 能力匹配

- Workflow/Task 只声明规范化能力，不引用 OpenClaw、Hermes 或 OpenAI 私有状态。
- Scheduler 在提交前匹配 capability snapshot；缺能力返回稳定错误或选择兼容 Endpoint。
- Agent Server 执行完成后先规范化 Event/Artifact，再进入 Stage 门禁。
- Tool/Agent 调用结果不直接写 Run；Runtime 通过 DurableStore 原子应用。

## 9. 事件与审计基线

核心业务事件至少包括：

```text
workflow.run.created
stage.planned
stage.activated
task.created
task.claimed
artifact.produced
artifact.validated
gate.evaluated
approval.required
approval.received
stage.rework_required
stage.succeeded
requirement.changed
run.pause_requested
run.paused
run.resume_requested
run.resumed
run.cancel_requested
agent.stop_requested
deployment.started
deployment.verified
run.completed
run.cancelled
```

约束：

- 事件名是领域事实，不复用 vendor event 名作为权威类型。
- 每个事件带 tenant、run、sequence、correlation、causation、producer 和幂等信息。
- 相同 Run 的 sequence 单调且唯一；客户端 SSE 从 sequence 补读。
- ignored/rejected/late 结果进入结构化审计，不得伪装为合法推进事件。
- 审计查询应能从最终 release 反向找到审批、测试、代码变更、技术决策和原始需求。

## 10. E2E-01：完整交付与测试返工

### 10.1 初始条件

- 已注册 `workflow.delivery.v1`、协调 Agent、专业 Agent 和 Mock DevOps Tool。
- Endpoint capability 满足 submit、status、event resume、artifact output；不依赖 vendor-specific 状态。
- Run 使用固定 tenant、Workflow version、Agent versions 和全局 deadline。
- 测试夹具配置 integration_test 第一次失败，修复后第二次通过。

### 10.2 命令序列

1. `CreateRun(idempotency_key=create-delivery-001)` 创建 Run、requirements Stage、首个 Task 和 checkpoint。
2. Product Agent 生成 RequirementSpec；阻塞问题通过 approval/external input Wait 澄清。
3. 需求门禁通过，原子完成 requirements 并激活 product_design。
4. 原型和 PRD Task 并行执行，fan-in 生成 TraceabilityMatrix 并等待产品审批。
5. 审批通过后激活 technical_design，生成技术方案、测试策略和 ImplementationPlan。
6. Coding Agent 按模块并行执行，合并结果形成固定 ChangeSet。
7. self_test 对当前 ChangeSet 执行并通过。
8. integration_test attempt 1 发现 blocker，保存失败 TestReport/DefectReport，阶段进入 rework_required。
9. Runtime 创建 implementation attempt 2；修复产生新 ChangeSet version，并重跑受影响自测。
10. integration_test attempt 2 对新 digest 执行并通过。
11. Runtime 创建部署审批 Wait；审批被唯一消费后执行 DevOps Tool。
12. 部署 accepted 后释放 Worker；poll Task 获得 release 成功并完成健康验证。
13. delivery_closure 生成最终追踪和审计清单；Run 原子进入 completed。

### 10.3 必需断言

- 重复 `CreateRun` 返回同一 run_id，只有一个 `workflow.run.created`。
- requirements 到 deployment 的每次 Stage 激活都有 causation Event 和固定输入 Artifact versions。
- integration_test attempt 1 的失败报告不可变，attempt 2 不覆盖它。
- 修复后的自测/集成报告绑定 ChangeSet version 2；旧报告不能通过新门禁。
- 发布审批重复提交只产生一个有效决定和一个部署 ToolExecution。
- 部署 API accepted 时 Run 不得 completed；只有健康报告通过后才能关闭。
- 最终 TraceabilityMatrix 覆盖所有 must requirement。
- `run.completed`、最终 checkpoint、delivery artifacts、Stage 状态和 `terminal_event_id` 同事务可见。
- 任意时刻终止 Worker，Lease 到期后流程可恢复且不重复确定性外部副作用。

### 10.4 最终投影

```text
Run.status = completed
required Stage latest attempts = succeeded
open WaitSubscription = 0
ready/running Task = 0
ToolExecution(deploy) = succeeded
AgentExecution nonterminal = 0
outcome_unknown Execution = 0
terminal_event_id = event(run.completed)
```

## 11. E2E-02：立即暂停与上下文恢复

### 11.1 初始条件

- Run 已完成 product_design，technical_design 正在执行。
- 最后成功 checkpoint `C42` 保存：确认的 RequirementSpec/PRD versions、最近成功用户消息、动态任务计划 generation 7。
- 一个 AgentExecution 正在远程运行，一个 Task 尚未领取。

### 11.2 竞争与恢复序列

1. 用户发送 `PauseRun(idempotency_key=pause-001, expected_version=42)`。
2. DurableStore 锁定 Run，保存 `suspended_from_status=running`，generation 递增到 8，冻结未执行 Task，Run 进入 paused 并提交 `run.paused`。
3. 提交后可靠 follow-up 请求远程 Agent stop；无论 Endpoint 是否支持 stop，暂停事务已经成立。
4. Scheduler/Worker 不能领取 generation 7 的待执行 Task。
5. 远程 Agent 在暂停后返回 completed；系统保存 remote terminal fact 和隔离的诊断引用，但不登记为当前 Stage 产物，generation fencing 拒绝其推进 Stage/Run。
6. 相同 pause key 重放返回首次结果；不同 payload 复用该 key 返回冲突。
7. 用户发送 `ResumeRun(idempotency_key=resume-001)`。
8. Runtime 重验 Workflow/Agent version、Artifact digest、deadline、权限和 C42 schema。
9. 若兼容，则基于 C42 创建 generation 9 的恢复 Task；若远程迟到产物可安全复用，必须先通过明确 reconcile/validation，而不是自动采用。
10. 恢复 Task 从最近成功用户消息、确认 Artifact 和未完成计划继续，Run 返回 queued/running。

### 11.3 必需断言

- `run.paused` 提交后没有新的可领取 Task；已领取 Task 的迟到完成不能推进权威状态。
- Pause 不等待远程 stop 响应，因此 Endpoint 不可用时仍能立即完成控制面暂停。
- C42 和 generation 7 的动态计划完整保留；系统不依赖原 Worker 内存。
- Stop accepted 不等于 remote cancelled，真实远程完成/取消事实均保留。
- Resume 前必须执行兼容性与 Artifact 完整性校验。
- 恢复只创建一组 generation 9 Task；重复 resume 不产生重复任务。
- 若 checkpoint 不兼容，Run 保持 paused 并返回稳定错误，不得从空上下文继续。

### 11.4 故障注入

至少在以下位置终止进程并重试：

- pause 事务提交前；
- pause 事务提交后、stop follow-up 前；
- stop 请求已接受但本地尚未记录时；
- resume 事务创建恢复 Task 前后。

所有结果必须收敛到“仍 paused”或“只有一组有效恢复 Task”，不得出现两个 generation 同时推进。

## 12. E2E-03：Cancel 与 Complete 竞争

### 12.1 初始条件

- Run 位于 delivery_closure，version 为 87。
- 最终 Task 正在完成；同时用户发起取消。
- 两个命令均携带独立幂等键，并以状态和 version 作为 CAS/锁后校验条件。

### 12.2 情形 A：Complete 先提交

1. CompleteTask 事务先锁定 Run。
2. 事务验证所有 Stage、Artifact Contract、Gate 和 Execution 均满足完成条件。
3. 同一事务写入最终 Artifact、Checkpoint、`run.completed` 和唯一 terminal_event_id，Run 进入 completed/version 88。
4. CancelRun 随后读取终态，返回 `AlreadyTerminal(completed)`，不得追加 `run.cancelled` 或关闭已完成产物。

断言：

- 最终状态为 completed；
- 全 Run 只有一个 terminal Event；
- 重复 cancel 稳定返回同一终态 no-op receipt；
- 不执行远程 stop/rollback，除非业务另行创建显式补偿命令。

### 12.3 情形 B：Cancel 先提交

1. CancelRun 事务先锁定 Run。
2. 同一事务将 Run 置为 cancelled/version 88，设置 terminal_event_id，取消待执行 Task、关闭 Wait 并使当前 generation 失效。
3. 事务提交可靠 stop follow-up；远程停止为尽力操作，不延迟本地终态。
4. CompleteTask 随后因 Run 已终态/版本失效被拒绝；可保存迟到执行证据，但不得登记为最终交付产物或写 `run.completed`。

断言：

- 最终状态为 cancelled；
- 全 Run 只有一个 terminal Event；
- completion 的业务状态更新、后继 Task 和 gate 结果均未提交；
- stop 失败或 remote completed 不得把 Run 改回 completed；
- 重复 complete 返回稳定 stale/terminal 结果，不产生部分提交。

### 12.4 并发测试方法

- 使用 barrier 让两个事务基于同一初始 version 开始。
- PostgreSQL 和 MySQL 分别重复执行足够次数，强制覆盖两种合法提交顺序。
- 每轮检查终态唯一约束、Event sequence、CommandReceipt、Task/Wait/Artifact 和 follow-up 数量。
- 禁止测试以“某一方永远优先”为通过条件；正确语义是首个合法终态事务获胜。

## 13. Provider 对等与故障注入矩阵

相同场景必须在 PostgreSQL 与 MySQL 8+/InnoDB 上运行，比较领域结果而不是数据库内部锁细节。

| 注入点 | 预期结果 |
| --- | --- |
| Task claim 前后 Worker 崩溃 | Lease 到期后唯一重新领取；旧 token 不能完成 |
| CompleteTask 事务任意写入点失败 | Event、Artifact、Stage、Checkpoint、后继 Task 全部回滚 |
| approval 重复/乱序 | 只消费一个 Wait；冲突决定被稳定拒绝 |
| Agent submit 响应丢失 | outcome_unknown/reconcile；不盲目创建第二个远程 Run |
| remote event batch 重放 | cursor/Event 去重；Stage 不重复推进 |
| deploy accepted 后进程崩溃 | 根据 external ref 查询；不重复非幂等部署 |
| health check 超时 | deployment 不成功；按策略重试、回滚或人工处理 |
| PostCommitHint 丢失 | 轮询路径最终执行 DurableFollowUp |
| SSE 客户端断线 | 从 Event sequence 补读，不影响权威执行 |
| deadline 与阶段完成竞争 | 首个合法事务获胜，唯一终态 |

对等断言至少比较：

```text
Run/Stage/Task/Wait terminal projections
ordered domain event types and causation graph
Artifact logical keys, versions and digests
CommandReceipt outcomes
ToolExecution/AgentExecution normalized outcomes
durable follow-up cardinality
```

数据库生成 ID、物理时间微小差异、锁等待次数和 SQL 错误文本不参与领域等价比较。

## 14. Mock Adapter 测试夹具

### 14.1 Mock Agent Server

必须可配置：

- accepted 后正常事件序列和最终 Artifact；
- 提交响应丢失但远程已创建；
- 重复、乱序、缺 source ID 和断流事件；
- approval/input required；
- stop unsupported、accepted、uncertain 和 stop/complete 竞争；
- capability/version 在不同 Execution 间变化；
- invalid payload、429、5xx 和超时。

Mock Server 必须是独立 Server 接口，测试不得通过 spawn CLI 验证生产 Adapter 语义。

### 14.2 Mock DevOps Tool

必须可配置：

- dry-run、accepted、polling、成功和失败；
- 相同 release ID 的幂等重放；
- 超时但部署实际成功；
- 健康验证失败；
- rollback 成功、失败和 outcome_unknown。

每个外部操作保存可查询的 operation/release ref，便于验证 Runtime 的对账逻辑。

## 15. 最小 API 验收视图

场景实现至少支持查询：

```text
POST /v1/runs
GET  /v1/runs/{run_id}
GET  /v1/runs/{run_id}/events?after_sequence=...
GET  /v1/runs/{run_id}/stages
GET  /v1/runs/{run_id}/artifacts
GET  /v1/runs/{run_id}/pending-actions
POST /v1/runs/{run_id}/events
POST /v1/runs/{run_id}/pause
POST /v1/runs/{run_id}/resume
POST /v1/runs/{run_id}/cancel
```

Run 查询视图至少返回：

- 当前 Run 状态、version、deadline 和 terminal reason；
- 当前/最近 Stage、attempt、负责人和进度；
- 待处理审批、澄清、人工复核和超时时间；
- 每个 logical artifact 的确认版本；
- 正在运行或 outcome_unknown 的 Agent/Tool Execution；
- 最近 checkpoint sequence 和恢复兼容性摘要。

查询接口不能把实时缓存当作权威来源。权限不足时隐藏 Artifact URI 和敏感内容，但保留调用方可见的阶段状态。

## 16. Definition of Done

`workflow.delivery.v1` 只有同时满足以下条件才算跑通：

1. 八个必需 Stage 均按 Artifact Contract 和门禁推进。
2. RequirementSpec 等价模型和端到端追踪矩阵可查询。
3. 测试失败能够创建不可变缺陷证据并完成跨 Stage 返工。
4. 部署经过审批、确定结果和健康验证，不以请求接受替代成功。
5. Pause 立即生效、保留成功上下文并能跨 Worker 恢复。
6. Cancel/Complete 竞争只产生一个权威终态。
7. 重复命令、事件、回调和 Adapter 结果不会产生重复业务推进。
8. Worker、Scheduler、API 任意重启后 Run 能从 DurableStore 继续。
9. PostgreSQL 与 MySQL 对三条 E2E 场景产生等价领域结果。
10. 最终 release 可反向追踪到需求、设计、代码、测试、审批和部署证据。

## 17. 后续实现产物

本文冻结后，后置动作按以下顺序展开：

1. `MIGRATION_DESIGN.md`：把领域模型落实为 PostgreSQL/MySQL 对等表、索引、约束和迁移顺序。
2. 初始化 Rust workspace 与 `domain`、`store-core`、`adapter-core` crate。
3. 生成 `workflow.delivery.v1` 类型化 fixture 和 JSON Schema。
4. 实现内存 Fake Provider，仅用于快速跑状态机与场景测试，不作为生产语义替代。
5. 实现 PostgreSQL Provider 和 Mock Agent/DevOps Server，跑通 E2E-01。
6. 实现 pause/resume/cancel 与故障注入，跑通 E2E-02、E2E-03。
7. 实现 MySQL Provider，复用同一 conformance/E2E suite 验证对等性。

实现阶段不得以修改测试预期来规避本场景契约；若发现契约无法实现，应通过版本化设计变更记录修订原因、兼容影响和迁移策略。
