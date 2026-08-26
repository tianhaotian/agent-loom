use agent_loom_domain::{AgentExecutionId, RunId, TenantId, ToolExecutionId, UnixMicros};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_cursor_preserves_retry_identity_and_ordering_key() {
        let candidate = DueWorkCandidate {
            tenant_id: TenantId::from_bytes([1; 16]),
            run_id: RunId::from_bytes([2; 16]),
            target: DueWorkTarget::Agent(AgentExecutionId::from_bytes([3; 16])),
            due_at: UnixMicros::new(4),
            expected_revision: 5,
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
}
