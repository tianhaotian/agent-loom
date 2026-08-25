use std::{future::Future, pin::Pin};

use agent_loom_domain::{RunId, RunSnapshot};

use crate::{
    ApplyEvent, ClaimTask, ClaimedTask, Committed, CompleteTask, ControlRun, CreateRun,
    EventCursor, EventPage, FailTask, QueryContext, RenewTaskLease, StoreResult,
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
