use crate::{JsonPayload, LogicalKey, StageStatus, TaskKind};

pub const EXECUTION_PLAN_SCHEMA_VERSION: u32 = 1;

/// Provider-independent, versioned plan used to materialize a new Run.
///
/// Version 1 intentionally models only the initial durable work. Later Tasks
/// may still be created dynamically by committed Events and checkpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub schema_version: u32,
    pub plan_key: LogicalKey,
    pub stages: Vec<ExecutionStageSpec>,
    pub initial_tasks: Vec<ExecutionTaskSpec>,
    pub extension: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionStageSpec {
    pub logical_key: LogicalKey,
    pub initial_status: StageStatus,
    pub assignee_kind: Option<String>,
    pub assignee_ref: Option<String>,
    pub input_contract: JsonPayload,
    pub output_contract: JsonPayload,
    pub policy: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionTaskSpec {
    pub logical_key: LogicalKey,
    pub stage_key: Option<LogicalKey>,
    pub handler_key: LogicalKey,
    pub kind: TaskKind,
    pub priority: i32,
    pub max_attempts: u32,
    pub input: JsonPayload,
}

impl ExecutionPlan {
    /// Validates relationships that every plan producer must preserve.
    ///
    /// # Errors
    ///
    /// Returns a stable shape error for unsupported versions, duplicate keys,
    /// invalid initial Stage states, or Tasks that cannot be scheduled.
    pub fn validate_shape(&self) -> Result<(), ExecutionPlanShapeError> {
        if self.schema_version != EXECUTION_PLAN_SCHEMA_VERSION {
            return Err(ExecutionPlanShapeError::UnsupportedSchemaVersion);
        }
        if self.initial_tasks.is_empty() {
            return Err(ExecutionPlanShapeError::NoInitialTasks);
        }
        for (index, stage) in self.stages.iter().enumerate() {
            if !matches!(
                stage.initial_status,
                StageStatus::Planned | StageStatus::Active
            ) {
                return Err(ExecutionPlanShapeError::InvalidInitialStageStatus);
            }
            if stage.assignee_kind.is_some() != stage.assignee_ref.is_some() {
                return Err(ExecutionPlanShapeError::IncompleteStageAssignee);
            }
            if self.stages[index + 1..]
                .iter()
                .any(|other| other.logical_key == stage.logical_key)
            {
                return Err(ExecutionPlanShapeError::DuplicateStageKey);
            }
        }
        for (index, task) in self.initial_tasks.iter().enumerate() {
            if task.max_attempts == 0 {
                return Err(ExecutionPlanShapeError::ZeroTaskAttempts);
            }
            if self.initial_tasks[index + 1..]
                .iter()
                .any(|other| other.logical_key == task.logical_key)
            {
                return Err(ExecutionPlanShapeError::DuplicateTaskKey);
            }
            if let Some(stage_key) = &task.stage_key {
                let stage = self
                    .stages
                    .iter()
                    .find(|stage| stage.logical_key == *stage_key)
                    .ok_or(ExecutionPlanShapeError::UnknownTaskStage)?;
                if stage.initial_status != StageStatus::Active {
                    return Err(ExecutionPlanShapeError::TaskStageNotActive);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionPlanShapeError {
    UnsupportedSchemaVersion,
    NoInitialTasks,
    InvalidInitialStageStatus,
    IncompleteStageAssignee,
    DuplicateStageKey,
    ZeroTaskAttempts,
    DuplicateTaskKey,
    UnknownTaskStage,
    TaskStageNotActive,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> JsonPayload {
        JsonPayload::from_validated_bytes(b"{}".to_vec())
    }

    fn task(stage_key: Option<&str>) -> ExecutionTaskSpec {
        ExecutionTaskSpec {
            logical_key: LogicalKey::parse("entry").expect("key"),
            stage_key: stage_key.map(|key| LogicalKey::parse(key).expect("stage key")),
            handler_key: LogicalKey::parse("research-agent").expect("handler key"),
            kind: TaskKind::AgentServer,
            priority: 0,
            max_attempts: 3,
            input: payload(),
        }
    }

    #[test]
    fn stage_less_agent_plan_is_valid() {
        let plan = ExecutionPlan {
            schema_version: EXECUTION_PLAN_SCHEMA_VERSION,
            plan_key: LogicalKey::parse("research").expect("key"),
            stages: Vec::new(),
            initial_tasks: vec![task(None)],
            extension: payload(),
        };

        assert_eq!(plan.validate_shape(), Ok(()));
    }

    #[test]
    fn initial_task_cannot_target_a_planned_stage() {
        let plan = ExecutionPlan {
            schema_version: EXECUTION_PLAN_SCHEMA_VERSION,
            plan_key: LogicalKey::parse("delivery").expect("key"),
            stages: vec![ExecutionStageSpec {
                logical_key: LogicalKey::parse("build").expect("key"),
                initial_status: StageStatus::Planned,
                assignee_kind: None,
                assignee_ref: None,
                input_contract: payload(),
                output_contract: payload(),
                policy: payload(),
            }],
            initial_tasks: vec![task(Some("build"))],
            extension: payload(),
        };

        assert_eq!(
            plan.validate_shape(),
            Err(ExecutionPlanShapeError::TaskStageNotActive)
        );
    }
}
