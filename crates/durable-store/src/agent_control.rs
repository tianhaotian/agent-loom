use agent_loom_domain::{AgentExecutionSnapshot, TenantId};

use crate::ExpectedRun;

/// One durable Agent execution whose Run control state requires a remote stop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentStopCandidate {
    pub tenant_id: TenantId,
    pub execution: AgentExecutionSnapshot,
    pub expected_run: ExpectedRun,
}

impl AgentStopCandidate {
    pub fn shape_is_valid(&self) -> bool {
        self.tenant_id == self.execution.tenant_id
            && self.execution.status == agent_loom_domain::AgentExecutionStatus::Stopping
            && self.execution.remote_run_ref.is_some()
            && self.execution.remote_protocol_version.is_some()
            && self.expected_run.run_id == self.execution.run_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentStopQuery {
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentStopPage {
    pub candidates: Vec<AgentStopCandidate>,
}

/// One due remote status reconciliation backed by an Agent execution version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentStatusCandidate {
    pub tenant_id: TenantId,
    pub execution: AgentExecutionSnapshot,
    pub expected_run: ExpectedRun,
}

impl AgentStatusCandidate {
    pub fn shape_is_valid(&self) -> bool {
        self.tenant_id == self.execution.tenant_id
            && self.execution.status == agent_loom_domain::AgentExecutionStatus::Reconciling
            && self.execution.remote_run_ref.is_some()
            && self.execution.remote_protocol_version.is_some()
            && self.execution.status_poll_at.is_some()
            && self.expected_run.run_id == self.execution.run_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentStatusQuery {
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentStatusPage {
    pub candidates: Vec<AgentStatusCandidate>,
}

/// One due resumable remote event read backed by an Agent cursor version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventCandidate {
    pub tenant_id: TenantId,
    pub execution: AgentExecutionSnapshot,
    pub expected_run: ExpectedRun,
}

impl AgentEventCandidate {
    pub fn shape_is_valid(&self) -> bool {
        self.tenant_id == self.execution.tenant_id
            && self.execution.status == agent_loom_domain::AgentExecutionStatus::Running
            && self.execution.remote_run_ref.is_some()
            && self.execution.remote_protocol_version.is_some()
            && self.execution.status_poll_at.is_some()
            && self.expected_run.run_id == self.execution.run_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentEventQuery {
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventPage {
    pub candidates: Vec<AgentEventCandidate>,
}

#[cfg(test)]
mod tests {
    use agent_loom_domain::{
        AgentExecutionId, AgentExecutionStatus, AgentVersionId, EndpointId, RunId, TaskId,
        UnixMicros,
    };

    use super::*;

    #[test]
    fn stop_candidate_requires_a_stopping_execution_with_remote_identity() {
        let tenant_id = TenantId::from_bytes([1; 16]);
        let run_id = RunId::from_bytes([2; 16]);
        let mut candidate = AgentStopCandidate {
            tenant_id,
            execution: AgentExecutionSnapshot {
                tenant_id,
                agent_execution_id: AgentExecutionId::from_bytes([3; 16]),
                run_id,
                stage_execution_id: None,
                task_id: TaskId::from_bytes([4; 16]),
                endpoint_id: EndpointId::from_bytes([5; 16]),
                agent_version_id: AgentVersionId::from_bytes([6; 16]),
                status: AgentExecutionStatus::Stopping,
                version: 2,
                remote_run_ref: Some("remote-run".to_owned()),
                remote_session_ref: None,
                remote_protocol_version: Some("1".to_owned()),
                status_poll_at: None,
                event_cursor: None,
                cursor_version: 0,
                retry_at: None,
                updated_at: UnixMicros::new(10),
            },
            expected_run: ExpectedRun {
                run_id,
                version: Some(3),
                execution_generation: Some(1),
            },
        };
        assert!(candidate.shape_is_valid());
        candidate.execution.remote_run_ref = None;
        assert!(!candidate.shape_is_valid());
    }
}
