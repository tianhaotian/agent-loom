use agent_loom_domain::{
    AgentExecutionId, EventId, RunId, RunStatus, TenantId, ToolExecutionId, UnixMicros, WaitId,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaintenanceKind {
    RunDeadline,
    WaitTimeout,
    ToolStale,
    AgentStale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceTarget {
    Run(RunId),
    Wait(WaitId),
    Tool(ToolExecutionId),
    Agent(AgentExecutionId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceCursor {
    pub due_at: UnixMicros,
    pub kind: MaintenanceKind,
    pub target_id: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceQuery {
    pub after: Option<MaintenanceCursor>,
    pub limit: u32,
    pub stale_after_micros: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceCandidate {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub target: MaintenanceTarget,
    pub due_at: UnixMicros,
    pub expected_revision: u64,
    pub run_version: u64,
    pub execution_generation: u64,
}

impl MaintenanceCandidate {
    pub const fn kind(self) -> MaintenanceKind {
        match self.target {
            MaintenanceTarget::Run(_) => MaintenanceKind::RunDeadline,
            MaintenanceTarget::Wait(_) => MaintenanceKind::WaitTimeout,
            MaintenanceTarget::Tool(_) => MaintenanceKind::ToolStale,
            MaintenanceTarget::Agent(_) => MaintenanceKind::AgentStale,
        }
    }

    pub const fn target_id_bytes(self) -> [u8; 16] {
        match self.target {
            MaintenanceTarget::Run(id) => id.into_bytes(),
            MaintenanceTarget::Wait(id) => id.into_bytes(),
            MaintenanceTarget::Tool(id) => id.into_bytes(),
            MaintenanceTarget::Agent(id) => id.into_bytes(),
        }
    }

    pub const fn cursor(self) -> MaintenanceCursor {
        MaintenanceCursor {
            due_at: self.due_at,
            kind: self.kind(),
            target_id: self.target_id_bytes(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenancePage {
    pub candidates: Vec<MaintenanceCandidate>,
    pub next_cursor: Option<MaintenanceCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApplyMaintenance {
    pub candidate: MaintenanceCandidate,
    pub event_id: EventId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceOutcome {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub target: MaintenanceTarget,
    pub run_status: RunStatus,
    pub applied_at: UnixMicros,
}
