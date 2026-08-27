use std::{error::Error, fmt};

use agent_loom_domain::{
    EXECUTION_PLAN_SCHEMA_VERSION, ExecutionPlan, ExecutionPlanShapeError, ExecutionStageSpec,
    ExecutionTaskSpec, JoinPolicy, JsonPayload, LogicalKey, RunId, StageExecutionId, StageStatus,
    TaskDependencySpec, TaskId, TaskKind, UnixMicros,
};
use agent_loom_durable_store::{InitialStage, InitialTask, InitialTaskDependency};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::identity::derived_id;

#[cfg(test)]
use crate::task_handler::TASK_INPUT_SCHEMA_V1;

const EXECUTION_PLAN_V1: &str = "agent-loom.execution-plan/v1";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionPlanProfile {
    schema: String,
    plan_key: String,
    #[serde(default)]
    stages: Vec<StageProfile>,
    initial_tasks: Vec<TaskProfile>,
    #[serde(default = "empty_object")]
    extension: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StageProfile {
    key: String,
    activation: StageActivation,
    #[serde(default)]
    assignee: Option<AssigneeProfile>,
    #[serde(default = "empty_object")]
    input_contract: Value,
    #[serde(default = "empty_object")]
    output_contract: Value,
    #[serde(default = "empty_object")]
    policy: Value,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StageActivation {
    Active,
    Planned,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssigneeProfile {
    kind: String,
    reference: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskProfile {
    key: String,
    #[serde(default)]
    stage_key: Option<String>,
    handler: String,
    kind: TaskKindProfile,
    #[serde(default)]
    priority: i32,
    #[serde(default = "default_max_attempts")]
    max_attempts: u32,
    #[serde(default = "empty_object")]
    input: Value,
    #[serde(default)]
    depends_on: Vec<TaskDependencyProfile>,
    #[serde(default)]
    join_policy: JoinPolicyProfile,
    #[serde(default)]
    context_projection: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskDependencyProfile {
    task: String,
    #[serde(default = "empty_object")]
    condition: Value,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum JoinPolicyProfile {
    #[default]
    All,
    Any,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TaskKindProfile {
    Model,
    Tool,
    AgentServer,
    ArtifactCheck,
    TimerWakeup,
}

#[derive(Debug)]
pub(crate) struct MaterializedExecutionPlan {
    pub checkpoint_state: JsonPayload,
    pub initial_stages: Vec<InitialStage>,
    pub initial_tasks: Vec<InitialTask>,
}

pub(crate) fn parse_execution_plan(spec: &JsonPayload) -> Result<ExecutionPlan, PlanError> {
    let profile: ExecutionPlanProfile =
        serde_json::from_slice(spec.as_bytes()).map_err(|_| PlanError::InvalidProfile)?;
    if profile.schema != EXECUTION_PLAN_V1 {
        return Err(PlanError::UnsupportedSchema);
    }
    let stages = profile
        .stages
        .into_iter()
        .map(|stage| {
            let (assignee_kind, assignee_ref) = stage.assignee.map_or((None, None), |assignee| {
                (Some(assignee.kind), Some(assignee.reference))
            });
            Ok(ExecutionStageSpec {
                logical_key: logical_key(stage.key)?,
                initial_status: match stage.activation {
                    StageActivation::Active => StageStatus::Active,
                    StageActivation::Planned => StageStatus::Planned,
                },
                assignee_kind,
                assignee_ref,
                input_contract: payload(&stage.input_contract)?,
                output_contract: payload(&stage.output_contract)?,
                policy: payload(&stage.policy)?,
            })
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    let initial_tasks = profile
        .initial_tasks
        .into_iter()
        .map(|task| {
            Ok(ExecutionTaskSpec {
                logical_key: logical_key(task.key)?,
                stage_key: task.stage_key.map(logical_key).transpose()?,
                handler_key: logical_key(task.handler)?,
                kind: match task.kind {
                    TaskKindProfile::Model => TaskKind::Model,
                    TaskKindProfile::Tool => TaskKind::Tool,
                    TaskKindProfile::AgentServer => TaskKind::AgentServer,
                    TaskKindProfile::ArtifactCheck => TaskKind::ArtifactCheck,
                    TaskKindProfile::TimerWakeup => TaskKind::TimerWakeup,
                },
                priority: task.priority,
                max_attempts: task.max_attempts,
                input: payload(&task.input)?,
                dependencies: task
                    .depends_on
                    .into_iter()
                    .map(|dependency| {
                        Ok(TaskDependencySpec {
                            task_key: logical_key(dependency.task)?,
                            condition: payload(&dependency.condition)?,
                        })
                    })
                    .collect::<Result<Vec<_>, PlanError>>()?,
                join_policy: match task.join_policy {
                    JoinPolicyProfile::All => JoinPolicy::All,
                    JoinPolicyProfile::Any => JoinPolicy::Any,
                },
                context_projection: task.context_projection,
            })
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    let plan = ExecutionPlan {
        schema_version: EXECUTION_PLAN_SCHEMA_VERSION,
        plan_key: logical_key(profile.plan_key)?,
        stages,
        initial_tasks,
        extension: payload(&profile.extension)?,
    };
    plan.validate_shape().map_err(PlanError::InvalidShape)?;
    Ok(plan)
}

pub(crate) fn materialize_execution_plan(
    plan: &ExecutionPlan,
    run_id: RunId,
    run_input: &Value,
    available_at: UnixMicros,
) -> Result<MaterializedExecutionPlan, PlanError> {
    plan.validate_shape().map_err(PlanError::InvalidShape)?;
    let extension: Value =
        serde_json::from_slice(plan.extension.as_bytes()).map_err(|_| PlanError::InvalidPayload)?;
    let checkpoint_state = payload(&json!({
        "execution_plan": {
            "schema_version": plan.schema_version,
            "plan_key": plan.plan_key.as_str(),
            "extension": extension,
        },
        "completed_steps": 0,
        "run_input": run_input,
    }))?;
    let initial_stages = plan
        .stages
        .iter()
        .map(|stage| InitialStage {
            stage_execution_id: stage_execution_id(run_id, &stage.logical_key),
            stage_key: stage.logical_key.clone(),
            definition_stage_key: stage.logical_key.clone(),
            status: stage.initial_status,
            attempt: 1,
            assignee_kind: stage.assignee_kind.clone(),
            assignee_ref: stage.assignee_ref.clone(),
            input_contract: stage.input_contract.clone(),
            output_contract: stage.output_contract.clone(),
            policy: stage.policy.clone(),
        })
        .collect();
    let initial_tasks = plan
        .initial_tasks
        .iter()
        .map(|task| {
            let task_spec: Value = serde_json::from_slice(task.input.as_bytes())
                .map_err(|_| PlanError::InvalidPayload)?;
            Ok(InitialTask {
                task_id: TaskId::from_bytes(derived_id(
                    "task",
                    &format!("{run_id}/{}", task.logical_key),
                )),
                stage_execution_id: task
                    .stage_key
                    .as_ref()
                    .map(|stage_key| stage_execution_id(run_id, stage_key)),
                logical_key: task.logical_key.clone(),
                kind: task.kind,
                priority: task.priority,
                available_at,
                max_attempts: task.max_attempts,
                input: crate::task_handler::encode_task_input(
                    &task.handler_key,
                    json!({
                        "task_spec": task_spec,
                        "run_input": run_input,
                    }),
                )
                .map_err(|_| PlanError::InvalidPayload)?,
                dependencies: task
                    .dependencies
                    .iter()
                    .map(|dependency| InitialTaskDependency {
                        prerequisite_task_id: TaskId::from_bytes(derived_id(
                            "task",
                            &format!("{run_id}/{}", dependency.task_key),
                        )),
                        condition: dependency.condition.clone(),
                    })
                    .collect(),
                join_policy: task.join_policy,
                context_projection: JsonPayload::from_validated_bytes(
                    serde_json::to_vec(&task.context_projection)
                        .map_err(|_| PlanError::InvalidPayload)?,
                ),
            })
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    Ok(MaterializedExecutionPlan {
        checkpoint_state,
        initial_stages,
        initial_tasks,
    })
}

pub(crate) fn materialize_plan_task_additions(
    current: &ExecutionPlan,
    revised: &ExecutionPlan,
    run_id: RunId,
    run_input: &Value,
    available_at: UnixMicros,
) -> Result<Vec<InitialTask>, PlanError> {
    current.validate_shape().map_err(PlanError::InvalidShape)?;
    revised.validate_shape().map_err(PlanError::InvalidShape)?;
    if current.schema_version != revised.schema_version
        || current.plan_key != revised.plan_key
        || current.stages != revised.stages
        || current.initial_tasks.iter().any(|existing| {
            revised
                .initial_tasks
                .iter()
                .find(|candidate| candidate.logical_key == existing.logical_key)
                != Some(existing)
        })
    {
        return Err(PlanError::UnsupportedRevisionMutation);
    }
    let mut materialized =
        materialize_execution_plan(revised, run_id, run_input, available_at)?.initial_tasks;
    materialized.retain(|task| {
        !current
            .initial_tasks
            .iter()
            .any(|existing| existing.logical_key == task.logical_key)
    });
    Ok(materialized)
}

pub(crate) fn stage_execution_id(run_id: RunId, stage_key: &LogicalKey) -> StageExecutionId {
    StageExecutionId::from_bytes(derived_id("stage", &format!("{run_id}/{stage_key}")))
}

const fn default_max_attempts() -> u32 {
    3
}

fn empty_object() -> Value {
    json!({})
}

fn logical_key(value: String) -> Result<LogicalKey, PlanError> {
    LogicalKey::parse(value).map_err(|_| PlanError::InvalidKey)
}

fn payload(value: &Value) -> Result<JsonPayload, PlanError> {
    serde_json::to_vec(value)
        .map(JsonPayload::from_validated_bytes)
        .map_err(|_| PlanError::InvalidPayload)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanError {
    InvalidProfile,
    UnsupportedSchema,
    InvalidKey,
    InvalidPayload,
    InvalidShape(ExecutionPlanShapeError),
    UnsupportedRevisionMutation,
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile => {
                formatter.write_str("Workflow spec is not a valid plan profile")
            }
            Self::UnsupportedSchema => {
                formatter.write_str("Workflow spec uses an unsupported plan schema")
            }
            Self::InvalidKey => formatter.write_str("Execution plan contains an invalid key"),
            Self::InvalidPayload => {
                formatter.write_str("Execution plan contains an invalid JSON payload")
            }
            Self::InvalidShape(error) => {
                write!(formatter, "Execution plan shape is invalid: {error:?}")
            }
            Self::UnsupportedRevisionMutation => formatter.write_str(
                "Plan revision may only append Tasks; existing Stages and Tasks are immutable",
            ),
        }
    }
}

impl Error for PlanError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(value: &Value) -> JsonPayload {
        JsonPayload::from_validated_bytes(serde_json::to_vec(value).expect("encode spec"))
    }

    #[test]
    fn delivery_plan_materializes_stages_and_bound_run_input() {
        let plan = parse_execution_plan(&spec(&json!({
            "schema": "agent-loom.execution-plan/v1",
            "plan_key": "delivery",
            "stages": [
                {"key": "requirements", "activation": "active"},
                {"key": "implementation", "activation": "planned"}
            ],
            "initial_tasks": [{
                "key": "requirements-entry",
                "stage_key": "requirements",
                "handler": "delivery-mvp",
                "kind": "agent_server",
                "priority": 10,
                "max_attempts": 3,
                "input": {"workflow": "delivery", "step": 0, "checkpoint_sequence": 1}
            }]
        })))
        .expect("parse plan");
        let materialized = materialize_execution_plan(
            &plan,
            RunId::from_bytes([7; 16]),
            &json!({"goal": "ship"}),
            UnixMicros::new(100),
        )
        .expect("materialize plan");

        assert_eq!(materialized.initial_stages.len(), 2);
        assert_eq!(materialized.initial_tasks.len(), 1);
        assert_eq!(materialized.initial_tasks[0].kind, TaskKind::AgentServer);
        let input: Value = serde_json::from_slice(materialized.initial_tasks[0].input.as_bytes())
            .expect("decode task input");
        assert_eq!(input["schema"], TASK_INPUT_SCHEMA_V1);
        assert_eq!(input["handler"], "delivery-mvp");
        assert_eq!(input["payload"]["run_input"]["goal"], "ship");
        assert_eq!(input["payload"]["task_spec"]["workflow"], "delivery");
    }

    #[test]
    fn stage_less_incident_plan_can_start_parallel_tasks() {
        let plan = parse_execution_plan(&spec(&json!({
            "schema": "agent-loom.execution-plan/v1",
            "plan_key": "incident-response",
            "initial_tasks": [
                {"key": "collect-logs", "handler": "incident", "kind": "tool", "input": {"source": "logs"}},
                {"key": "inspect-metrics", "handler": "incident", "kind": "tool", "input": {"source": "metrics"}}
            ],
            "extension": {"scenario": "operations"}
        })))
        .expect("parse plan");
        let materialized = materialize_execution_plan(
            &plan,
            RunId::from_bytes([8; 16]),
            &json!({"incident": "INC-1"}),
            UnixMicros::new(200),
        )
        .expect("materialize plan");

        assert!(materialized.initial_stages.is_empty());
        assert_eq!(materialized.initial_tasks.len(), 2);
        assert_ne!(
            materialized.initial_tasks[0].task_id,
            materialized.initial_tasks[1].task_id
        );
    }

    #[test]
    fn unsupported_profile_is_rejected_before_materialization() {
        let error = parse_execution_plan(&spec(&json!({
            "schema": "agent-loom.execution-plan/v2",
            "plan_key": "future",
            "initial_tasks": [{"key": "entry", "handler": "future", "kind": "model"}]
        })))
        .expect_err("unsupported schema");

        assert_eq!(error, PlanError::UnsupportedSchema);
    }

    #[test]
    fn plan_revision_materializes_only_append_only_tasks() {
        let current = parse_execution_plan(&spec(&json!({
            "schema": "agent-loom.execution-plan/v1",
            "plan_key": "adaptive",
            "initial_tasks": [
                {"key": "entry", "handler": "adaptive", "kind": "model"}
            ]
        })))
        .expect("parse current Plan");
        let revised = parse_execution_plan(&spec(&json!({
            "schema": "agent-loom.execution-plan/v1",
            "plan_key": "adaptive",
            "initial_tasks": [
                {"key": "entry", "handler": "adaptive", "kind": "model"},
                {"key": "follow-up", "handler": "adaptive", "kind": "tool", "depends_on": [{"task": "entry"}]}
            ]
        })))
        .expect("parse revised Plan");

        let additions = materialize_plan_task_additions(
            &current,
            &revised,
            RunId::from_bytes([9; 16]),
            &json!({"goal": "adapt"}),
            UnixMicros::new(300),
        )
        .expect("materialize appended Task");
        assert_eq!(additions.len(), 1);
        assert_eq!(additions[0].logical_key.as_str(), "follow-up");
        assert_eq!(additions[0].dependencies.len(), 1);

        let mut changed = revised.clone();
        changed.initial_tasks[0].priority = 1;
        assert_eq!(
            materialize_plan_task_additions(
                &current,
                &changed,
                RunId::from_bytes([9; 16]),
                &json!({}),
                UnixMicros::new(300),
            ),
            Err(PlanError::UnsupportedRevisionMutation)
        );
    }
}
