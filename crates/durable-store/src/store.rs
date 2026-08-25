use std::{future::Future, pin::Pin};

use agent_loom_domain::{AgentExecutionSnapshot, RunId, RunSnapshot, ToolExecutionSnapshot};

use crate::{
    AgentEventBatchOutcome, AppendAgentEvents, ApplyEvent, ClaimTask, ClaimedTask, Committed,
    CompleteTask, ControlRun, CreateRun, EventCursor, EventPage, FailTask, PrepareAgentExecution,
    PrepareToolExecution, QueryContext, RecordAgentOutcome, RecordAgentSubmission,
    RecordToolOutcome, RenewTaskLease, StoreResult,
};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = StoreResult<T>> + Send + 'a>>;

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreCapabilities {
    pub wakeup_notification: bool,
    pub read_replica: bool,
    pub json_path_query: bool,
    pub partition_management: bool,
    pub full_text_search: bool,
}

impl StoreCapabilities {
    pub const PORTABLE_BASELINE: Self = Self {
        wakeup_notification: false,
        read_replica: false,
        json_path_query: false,
        partition_management: false,
        full_text_search: false,
    };
}

pub trait DurableStore: Send + Sync {
    fn capabilities(&self) -> StoreCapabilities;

    fn create_run<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: CreateRun,
    ) -> StoreFuture<'a, Committed<RunSnapshot>>;

    fn get_run<'a>(
        &'a self,
        context: &'a QueryContext,
        run_id: RunId,
    ) -> StoreFuture<'a, Option<RunSnapshot>>;

    fn list_events<'a>(
        &'a self,
        context: &'a QueryContext,
        cursor: EventCursor,
    ) -> StoreFuture<'a, EventPage>;

    fn claim_task<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: ClaimTask,
    ) -> StoreFuture<'a, Option<Committed<ClaimedTask>>>;

    fn renew_task_lease<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: RenewTaskLease,
    ) -> StoreFuture<'a, Committed<ClaimedTask>>;

    fn complete_task<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: CompleteTask,
    ) -> StoreFuture<'a, Committed<RunSnapshot>>;

    fn fail_task<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: FailTask,
    ) -> StoreFuture<'a, Committed<RunSnapshot>>;

    fn apply_event<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: ApplyEvent,
    ) -> StoreFuture<'a, Committed<RunSnapshot>>;

    fn prepare_tool_execution<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: PrepareToolExecution,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>>;

    fn record_tool_outcome<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: RecordToolOutcome,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>>;

    fn prepare_agent_execution<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: PrepareAgentExecution,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>>;

    fn record_agent_submission<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: RecordAgentSubmission,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>>;

    fn append_agent_events<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: AppendAgentEvents,
    ) -> StoreFuture<'a, Committed<AgentEventBatchOutcome>>;

    fn record_agent_outcome<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: RecordAgentOutcome,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>>;

    fn pause_run<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: ControlRun,
    ) -> StoreFuture<'a, Committed<RunSnapshot>>;

    fn resume_run<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: ControlRun,
    ) -> StoreFuture<'a, Committed<RunSnapshot>>;

    fn cancel_run<'a>(
        &'a self,
        context: &'a crate::CommandContext,
        command: ControlRun,
    ) -> StoreFuture<'a, Committed<RunSnapshot>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_store_trait_is_dyn_compatible() {
        fn accepts(_: Option<&dyn DurableStore>) {}
        accepts(None);
    }
}
