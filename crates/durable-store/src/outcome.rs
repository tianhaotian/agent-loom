use agent_loom_domain::{
    AgentExecutionId, EventId, RunId, TaskId, TaskSnapshot, ToolExecutionId, UnixMicros,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommandDisposition {
    Applied,
    Duplicate,
    NoOp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableFollowUp {
    Task { task_id: TaskId },
    ReconcileAgent { execution_id: AgentExecutionId },
    StopAgent { execution_id: AgentExecutionId },
    ReconcileTool { execution_id: ToolExecutionId },
    CompensateTool { execution_id: ToolExecutionId },
    ScanDueWork { not_before: UnixMicros },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PostCommitHint {
    WakeWorkers,
    WakeScheduler,
    RunEventsAvailable { run_id: RunId },
    InvalidateRunCache { run_id: RunId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed<T> {
    pub disposition: CommandDisposition,
    pub value: T,
    pub event_ids: Vec<EventId>,
    pub durable_follow_ups: Vec<DurableFollowUp>,
    pub post_commit_hints: Vec<PostCommitHint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedTask {
    pub task: TaskSnapshot,
    pub run_version: u64,
    pub lease_expires_at: UnixMicros,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventPage {
    pub events: Vec<agent_loom_domain::EventRecord>,
    pub next_after_sequence: Option<u64>,
}
