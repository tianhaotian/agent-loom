use agent_loom_domain::{
    AgentExecutionId, EventId, RunId, RunStatus, TaskId, TaskKind, TenantId, ToolExecutionId,
    UnixMicros,
};

use crate::{ExpectedRun, NewTask};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DueWorkKind {
    ToolRetry,
    AgentRetry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DueWorkTarget {
    Tool(ToolExecutionId),
    Agent(AgentExecutionId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DueWorkCursor {
    pub due_at: UnixMicros,
    pub kind: DueWorkKind,
    pub execution_id: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DueWorkQuery {
    pub after: Option<DueWorkCursor>,
    pub limit: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DueWorkCandidate {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub target: DueWorkTarget,
    pub due_at: UnixMicros,
    pub expected_revision: u64,
    pub run_version: u64,
    pub execution_generation: u64,
    pub checkpoint_sequence: u64,
}

impl DueWorkCandidate {
    pub const fn kind(self) -> DueWorkKind {
        match self.target {
            DueWorkTarget::Tool(_) => DueWorkKind::ToolRetry,
            DueWorkTarget::Agent(_) => DueWorkKind::AgentRetry,
        }
    }

    pub const fn execution_id_bytes(self) -> [u8; 16] {
        match self.target {
            DueWorkTarget::Tool(id) => id.into_bytes(),
            DueWorkTarget::Agent(id) => id.into_bytes(),
        }
    }

    pub const fn cursor(self) -> DueWorkCursor {
        DueWorkCursor {
            due_at: self.due_at,
            kind: self.kind(),
            execution_id: self.execution_id_bytes(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueWorkPage {
    pub candidates: Vec<DueWorkCandidate>,
    pub next_cursor: Option<DueWorkCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyDueWork {
    pub candidate: DueWorkCandidate,
    pub expected_run: ExpectedRun,
    pub recovery_task: NewTask,
    pub applied_event_id: EventId,
}

impl ApplyDueWork {
    pub fn shape_is_valid(&self) -> bool {
        self.expected_run.run_id == self.candidate.run_id
            && self.expected_run.version == Some(self.candidate.run_version)
            && self.expected_run.execution_generation == Some(self.candidate.execution_generation)
            && self.recovery_task.kind == TaskKind::Reconcile
            && self.recovery_task.generation == self.candidate.execution_generation
            && self.recovery_task.based_on_checkpoint_sequence == self.candidate.checkpoint_sequence
            && self.candidate.checkpoint_sequence > 0
            && self.recovery_task.max_attempts > 0
            && self.recovery_task.created_event_id == self.applied_event_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DueWorkOutcome {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub target: DueWorkTarget,
    pub recovery_task_id: TaskId,
    pub run_status: RunStatus,
    pub execution_revision: u64,
    pub applied_at: UnixMicros,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_loom_domain::{JsonPayload, LogicalKey};

    #[test]
    fn candidate_cursor_preserves_retry_identity_and_ordering_key() {
        let candidate = DueWorkCandidate {
            tenant_id: TenantId::from_bytes([1; 16]),
            run_id: RunId::from_bytes([2; 16]),
            target: DueWorkTarget::Agent(AgentExecutionId::from_bytes([3; 16])),
            due_at: UnixMicros::new(4),
            expected_revision: 5,
            run_version: 6,
            execution_generation: 7,
            checkpoint_sequence: 8,
        };
        assert_eq!(
            candidate.cursor(),
            DueWorkCursor {
                due_at: UnixMicros::new(4),
                kind: DueWorkKind::AgentRetry,
                execution_id: [3; 16],
            }
        );
    }

    #[test]
    fn apply_due_work_requires_scanned_fences_and_reconcile_task() {
        let candidate = DueWorkCandidate {
            tenant_id: TenantId::from_bytes([1; 16]),
            run_id: RunId::from_bytes([2; 16]),
            target: DueWorkTarget::Tool(ToolExecutionId::from_bytes([3; 16])),
            due_at: UnixMicros::new(4),
            expected_revision: 1,
            run_version: 5,
            execution_generation: 6,
            checkpoint_sequence: 7,
        };
        let event_id = EventId::from_bytes([8; 16]);
        let mut command = ApplyDueWork {
            candidate,
            expected_run: ExpectedRun {
                run_id: candidate.run_id,
                version: Some(5),
                execution_generation: Some(6),
            },
            recovery_task: NewTask {
                task_id: TaskId::from_bytes([9; 16]),
                stage_execution_id: None,
                logical_key: LogicalKey::parse("due/tool-retry").expect("logical key"),
                kind: TaskKind::Reconcile,
                generation: 6,
                based_on_checkpoint_sequence: 7,
                priority: 1,
                available_at: UnixMicros::new(4),
                max_attempts: 3,
                input: JsonPayload::from_validated_bytes(b"{}".to_vec()),
                deadline: None,
                created_event_id: event_id,
            },
            applied_event_id: event_id,
        };
        assert!(command.shape_is_valid());
        command.recovery_task.kind = TaskKind::Tool;
        assert!(!command.shape_is_valid());
    }
}
