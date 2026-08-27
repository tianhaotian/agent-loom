use agent_loom_domain::{AgentExecutionId, ToolExecutionId};
use agent_loom_domain::{
    AgentExecutionSnapshot, ArtifactRefSnapshot, OutboxMessage, RunId, RunSnapshot,
    StageExecutionSnapshot, ToolExecutionSnapshot, WaitSnapshot, WorkflowId, WorkflowSnapshot,
};
use agent_loom_durable_store::{
    AgentEventBatchOutcome, AgentEventPage, AgentEventQuery, AgentInvocation, AgentStatusPage,
    AgentStatusQuery, AgentStopPage, AgentStopQuery, AppendAgentEvents, ApplyDueWork, ApplyEvent,
    ApplyMaintenance, BeginAgentResubmission, BeginToolRetryAttempt, ClaimOutbox, ClaimTask,
    ClaimedTask, CommandContext, Committed, CompleteTask, ControlRun, CreateRun, DueWorkOutcome,
    DueWorkPage, DueWorkQuery, DurableStore, EventCursor, EventPage, FailTask, LeaseReclaimOutcome,
    MaintenanceOutcome, MaintenancePage, MaintenanceQuery, PrepareAgentExecution,
    PrepareToolExecution, QueryContext, ReclaimExpiredLease, RecordAgentOutcome,
    RecordAgentSubmission, RecordOutboxDelivery, RecordToolOutcome, RenewTaskLease, RetryClass,
    StoreCapabilities, StoreError, StoreErrorCode, StoreFuture, StoreResult, ToolInvocation,
};
use deadpool_postgres::{Object, Pool};

use crate::{PostgresTransactionConfig, PostgresTransactionExecutor, capabilities};

/// Connection-pooled PostgreSQL implementation of the portable `DurableStore`
/// boundary. A connection is checked out only for one Store operation and is
/// returned after that operation's transaction or query has completed.
#[derive(Clone, Debug)]
pub struct PostgresStore {
    pool: Pool,
    executor: PostgresTransactionExecutor,
}

impl PostgresStore {
    pub fn new(pool: Pool) -> Self {
        Self::with_transaction_config(pool, PostgresTransactionConfig::default())
    }

    pub const fn with_transaction_config(
        pool: Pool,
        transaction_config: PostgresTransactionConfig,
    ) -> Self {
        Self {
            pool,
            executor: PostgresTransactionExecutor::new(transaction_config),
        }
    }

    pub const fn pool(&self) -> &Pool {
        &self.pool
    }

    async fn connection(&self) -> StoreResult<Object> {
        self.pool.get().await.map_err(|_| {
            StoreError::new(
                StoreErrorCode::StoreUnavailable,
                RetryClass::Backoff,
                "PostgreSQL connection pool is unavailable",
            )
        })
    }
}

impl DurableStore for PostgresStore {
    fn capabilities(&self) -> StoreCapabilities {
        capabilities()
    }

    fn create_run<'a>(
        &'a self,
        context: &'a CommandContext,
        command: CreateRun,
    ) -> StoreFuture<'a, Committed<RunSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .create_run(&mut client, context, command)
                .await
        })
    }

    fn get_run<'a>(
        &'a self,
        context: &'a QueryContext,
        run_id: RunId,
    ) -> StoreFuture<'a, Option<RunSnapshot>> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor.get_run(&client, context, run_id).await
        })
    }

    fn get_workflow<'a>(
        &'a self,
        context: &'a QueryContext,
        workflow_id: WorkflowId,
    ) -> StoreFuture<'a, Option<WorkflowSnapshot>> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor
                .get_workflow(&client, context, workflow_id)
                .await
        })
    }

    fn list_stages<'a>(
        &'a self,
        context: &'a QueryContext,
        run_id: RunId,
    ) -> StoreFuture<'a, Vec<StageExecutionSnapshot>> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor.list_stages(&client, context, run_id).await
        })
    }

    fn list_artifacts<'a>(
        &'a self,
        context: &'a QueryContext,
        run_id: RunId,
    ) -> StoreFuture<'a, Vec<ArtifactRefSnapshot>> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor.list_artifacts(&client, context, run_id).await
        })
    }

    fn list_waits<'a>(
        &'a self,
        context: &'a QueryContext,
        run_id: RunId,
    ) -> StoreFuture<'a, Vec<WaitSnapshot>> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor.list_waits(&client, context, run_id).await
        })
    }

    fn list_events<'a>(
        &'a self,
        context: &'a QueryContext,
        cursor: EventCursor,
    ) -> StoreFuture<'a, EventPage> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor.list_events(&client, context, cursor).await
        })
    }

    fn scan_due_work<'a>(
        &'a self,
        context: &'a QueryContext,
        query: DueWorkQuery,
    ) -> StoreFuture<'a, DueWorkPage> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor.scan_due_work(&client, context, query).await
        })
    }

    fn scan_maintenance<'a>(
        &'a self,
        context: &'a QueryContext,
        query: MaintenanceQuery,
    ) -> StoreFuture<'a, MaintenancePage> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor
                .scan_maintenance(&client, context, query)
                .await
        })
    }

    fn scan_agent_stops<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentStopQuery,
    ) -> StoreFuture<'a, AgentStopPage> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor
                .scan_agent_stops(&client, context, query)
                .await
        })
    }

    fn scan_agent_status<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentStatusQuery,
    ) -> StoreFuture<'a, AgentStatusPage> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor
                .scan_agent_status(&client, context, query)
                .await
        })
    }

    fn scan_agent_events<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentEventQuery,
    ) -> StoreFuture<'a, AgentEventPage> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor
                .scan_agent_events(&client, context, query)
                .await
        })
    }

    fn claim_outbox<'a>(
        &'a self,
        context: &'a QueryContext,
        command: ClaimOutbox,
    ) -> StoreFuture<'a, Option<OutboxMessage>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .claim_outbox(&mut client, context, command)
                .await
        })
    }

    fn record_outbox_delivery<'a>(
        &'a self,
        context: &'a QueryContext,
        command: RecordOutboxDelivery,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .record_outbox_delivery(&mut client, context, command)
                .await
        })
    }

    fn get_tool_invocation<'a>(
        &'a self,
        context: &'a QueryContext,
        execution_id: ToolExecutionId,
    ) -> StoreFuture<'a, Option<ToolInvocation>> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor
                .get_tool_invocation(&client, context, execution_id)
                .await
        })
    }

    fn get_agent_invocation<'a>(
        &'a self,
        context: &'a QueryContext,
        execution_id: AgentExecutionId,
    ) -> StoreFuture<'a, Option<AgentInvocation>> {
        Box::pin(async move {
            let client = self.connection().await?;
            self.executor
                .get_agent_invocation(&client, context, execution_id)
                .await
        })
    }

    fn apply_due_work<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ApplyDueWork,
    ) -> StoreFuture<'a, Committed<DueWorkOutcome>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .apply_due_work(&mut client, context, command)
                .await
        })
    }

    fn apply_maintenance<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ApplyMaintenance,
    ) -> StoreFuture<'a, Option<MaintenanceOutcome>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .apply_maintenance(&mut client, context, command)
                .await
        })
    }

    fn claim_task<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ClaimTask,
    ) -> StoreFuture<'a, Option<Committed<ClaimedTask>>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .claim_task(&mut client, context, command)
                .await
        })
    }

    fn renew_task_lease<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RenewTaskLease,
    ) -> StoreFuture<'a, Committed<ClaimedTask>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .renew_task_lease(&mut client, context, command)
                .await
        })
    }

    fn reclaim_expired_lease<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ReclaimExpiredLease,
    ) -> StoreFuture<'a, Option<Committed<LeaseReclaimOutcome>>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .reclaim_expired_lease(&mut client, context, command)
                .await
        })
    }

    fn complete_task<'a>(
        &'a self,
        context: &'a CommandContext,
        command: CompleteTask,
    ) -> StoreFuture<'a, Committed<RunSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .complete_task(&mut client, context, command)
                .await
        })
    }

    fn fail_task<'a>(
        &'a self,
        context: &'a CommandContext,
        command: FailTask,
    ) -> StoreFuture<'a, Committed<RunSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor.fail_task(&mut client, context, command).await
        })
    }

    fn apply_event<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ApplyEvent,
    ) -> StoreFuture<'a, Committed<RunSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .apply_event(&mut client, context, command)
                .await
        })
    }

    fn prepare_tool_execution<'a>(
        &'a self,
        context: &'a CommandContext,
        command: PrepareToolExecution,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .prepare_tool_execution(&mut client, context, command)
                .await
        })
    }

    fn record_tool_outcome<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordToolOutcome,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .record_tool_outcome(&mut client, context, command)
                .await
        })
    }

    fn begin_tool_retry_attempt<'a>(
        &'a self,
        context: &'a CommandContext,
        command: BeginToolRetryAttempt,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .begin_tool_retry_attempt(&mut client, context, command)
                .await
        })
    }

    fn prepare_agent_execution<'a>(
        &'a self,
        context: &'a CommandContext,
        command: PrepareAgentExecution,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .prepare_agent_execution(&mut client, context, command)
                .await
        })
    }

    fn record_agent_submission<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordAgentSubmission,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .record_agent_submission(&mut client, context, command)
                .await
        })
    }

    fn begin_agent_resubmission<'a>(
        &'a self,
        context: &'a CommandContext,
        command: BeginAgentResubmission,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .begin_agent_resubmission(&mut client, context, command)
                .await
        })
    }

    fn append_agent_events<'a>(
        &'a self,
        context: &'a CommandContext,
        command: AppendAgentEvents,
    ) -> StoreFuture<'a, Committed<AgentEventBatchOutcome>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .append_agent_events(&mut client, context, command)
                .await
        })
    }

    fn record_agent_outcome<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordAgentOutcome,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .record_agent_outcome(&mut client, context, command)
                .await
        })
    }

    fn pause_run<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ControlRun,
    ) -> StoreFuture<'a, Committed<RunSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor.pause_run(&mut client, context, command).await
        })
    }

    fn resume_run<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ControlRun,
    ) -> StoreFuture<'a, Committed<RunSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .resume_run(&mut client, context, command)
                .await
        })
    }

    fn cancel_run<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ControlRun,
    ) -> StoreFuture<'a, Committed<RunSnapshot>> {
        Box::pin(async move {
            let mut client = self.connection().await?;
            self.executor
                .cancel_run(&mut client, context, command)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use agent_loom_durable_store::{DurableStore, RetryClass, StoreErrorCode};
    use deadpool_postgres::{Config, Runtime};
    use tokio_postgres::NoTls;

    use super::*;

    fn test_pool() -> Pool {
        let mut config = Config::new();
        config.dbname = Some("agent_loom_test".to_owned());
        config
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .expect("pool configuration")
    }

    #[test]
    fn pooled_store_is_a_dyn_durable_store() {
        fn accepts(_: &dyn DurableStore) {}

        let store = PostgresStore::new(test_pool());
        accepts(&store);
        assert_eq!(store.capabilities(), crate::capabilities());
    }

    #[tokio::test]
    async fn pool_checkout_failure_is_redacted_and_retryable() {
        let pool = test_pool();
        pool.close();
        let store = PostgresStore::new(pool);
        let error = store
            .connection()
            .await
            .expect_err("a closed pool must fail");
        assert_eq!(error.code, StoreErrorCode::StoreUnavailable);
        assert_eq!(error.retry, RetryClass::Backoff);
        assert_eq!(
            error.safe_message,
            "PostgreSQL connection pool is unavailable"
        );
    }
}
