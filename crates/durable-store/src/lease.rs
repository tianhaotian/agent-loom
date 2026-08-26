use agent_loom_domain::{EventId, RunId, TaskId, TenantId, UnixMicros};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReclaimExpiredLease {
    pub reclaimed_event_id: EventId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseExpiryAction {
    RetryScheduled,
    DeadLettered,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseReclaimOutcome {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt: u32,
    pub action: LeaseExpiryAction,
    pub reclaimed_at: UnixMicros,
}
