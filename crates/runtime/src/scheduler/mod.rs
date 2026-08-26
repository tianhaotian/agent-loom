use std::{error::Error, fmt};

use agent_loom_domain::{
    CommandId, CorrelationId, Digest, EventId, IdempotencyKey, JsonPayload, LogicalKey, ScopeKey,
    TaskId, TaskKind,
};
use agent_loom_durable_store::{
    ApplyDueWork, CommandContext, CommandDisposition, Committed, DueWorkCandidate, DueWorkCursor,
    DueWorkKind, DueWorkOutcome, DueWorkPage, DueWorkQuery, ExpectedRun, NewTask, QueryContext,
    StoreError, StoreFuture,
};
use sha2::{Digest as _, Sha256};

/// Minimal Store surface used by the due-work scheduler.
pub trait DueWorkSchedulerStore: Send + Sync {
    fn scan_due_work<'a>(
        &'a self,
        context: &'a QueryContext,
        query: DueWorkQuery,
    ) -> StoreFuture<'a, DueWorkPage>;

    fn apply_due_work<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ApplyDueWork,
    ) -> StoreFuture<'a, Committed<DueWorkOutcome>>;
}

impl<T> DueWorkSchedulerStore for T
where
    T: agent_loom_durable_store::DurableStore + ?Sized,
{
    fn scan_due_work<'a>(
        &'a self,
        context: &'a QueryContext,
        query: DueWorkQuery,
    ) -> StoreFuture<'a, DueWorkPage> {
        agent_loom_durable_store::DurableStore::scan_due_work(self, context, query)
    }

    fn apply_due_work<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ApplyDueWork,
    ) -> StoreFuture<'a, Committed<DueWorkOutcome>> {
        agent_loom_durable_store::DurableStore::apply_due_work(self, context, command)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedDueWork {
    pub context: CommandContext,
    pub command: ApplyDueWork,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueWorkPlanError {
    pub safe_message: String,
}

impl fmt::Display for DueWorkPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl Error for DueWorkPlanError {}

pub trait DueWorkPlanner: Send + Sync {
    /// Produces stable command, receipt and recovery Task identities for one
    /// scanned candidate. Replanning the same candidate must return the same
    /// values so a Scheduler crash can safely replay the command.
    ///
    /// # Errors
    ///
    /// Returns a safe planning error when generated command metadata cannot
    /// satisfy the portable Store contract.
    fn plan(&self, candidate: DueWorkCandidate) -> Result<PlannedDueWork, DueWorkPlanError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeterministicDueWorkPlanner {
    actor_ref: String,
    recovery_task_priority: i32,
    recovery_task_max_attempts: u32,
}

impl DeterministicDueWorkPlanner {
    /// Creates a planner for stable due-work recovery commands.
    ///
    /// # Errors
    ///
    /// Returns a planning error when the actor reference is empty or the
    /// recovery Task attempt limit is zero.
    pub fn new(
        actor_ref: impl Into<String>,
        recovery_task_priority: i32,
        recovery_task_max_attempts: u32,
    ) -> Result<Self, DueWorkPlanError> {
        let actor_ref = actor_ref.into();
        if actor_ref.is_empty() {
            return Err(plan_error("Scheduler actor reference must not be empty"));
        }
        if recovery_task_max_attempts == 0 {
            return Err(plan_error("recovery Task max attempts must be positive"));
        }
        Ok(Self {
            actor_ref,
            recovery_task_priority,
            recovery_task_max_attempts,
        })
    }
}

impl DueWorkPlanner for DeterministicDueWorkPlanner {
    fn plan(&self, candidate: DueWorkCandidate) -> Result<PlannedDueWork, DueWorkPlanError> {
        let kind = due_work_kind(candidate.kind());
        let execution_id = hex(candidate.execution_id_bytes());
        let identity = format!(
            "due-work/{kind}/{execution_id}/{}/{}",
            candidate.due_at.get(),
            candidate.expected_revision
        );
        let event_id = EventId::from_bytes(derived_id("event", &identity));
        let context = CommandContext {
            tenant_id: candidate.tenant_id,
            command_id: CommandId::from_bytes(derived_id("command", &identity)),
            correlation_id: CorrelationId::from_bytes(derived_id(
                "correlation",
                &candidate.run_id.to_string(),
            )),
            actor_ref: self.actor_ref.clone(),
            scope: parse_scope("scheduler.due_work")?,
            idempotency_key: parse_idempotency(identity.clone())?,
            request_hash: hash(&format!("apply/{identity}")),
        };
        let input = format!(
            "{{\"due_work_kind\":\"{kind}\",\"execution_id\":\"{execution_id}\",\"expected_revision\":{}}}",
            candidate.expected_revision
        );
        let command = ApplyDueWork {
            candidate,
            expected_run: ExpectedRun {
                run_id: candidate.run_id,
                version: Some(candidate.run_version),
                execution_generation: Some(candidate.execution_generation),
            },
            recovery_task: NewTask {
                task_id: TaskId::from_bytes(derived_id("task", &identity)),
                stage_execution_id: candidate.stage_execution_id,
                logical_key: parse_logical(format!(
                    "reconcile/{kind}/{execution_id}/{}",
                    candidate.expected_revision
                ))?,
                kind: TaskKind::Reconcile,
                generation: candidate.execution_generation,
                based_on_checkpoint_sequence: candidate.checkpoint_sequence,
                priority: self.recovery_task_priority,
                available_at: candidate.due_at,
                max_attempts: self.recovery_task_max_attempts,
                input: JsonPayload::from_validated_bytes(input.into_bytes()),
                deadline: None,
                created_event_id: event_id,
            },
            applied_event_id: event_id,
        };
        Ok(PlannedDueWork { context, command })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DueWorkSchedulerConfig {
    pub page_size: u32,
}

impl Default for DueWorkSchedulerConfig {
    fn default() -> Self {
        Self { page_size: 100 }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CandidateFailure {
    Planning {
        candidate: DueWorkCandidate,
        error: DueWorkPlanError,
    },
    Store {
        candidate: DueWorkCandidate,
        error: StoreError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueWorkTickReport {
    pub scanned: usize,
    pub applied: usize,
    pub duplicates: usize,
    pub no_ops: usize,
    pub failures: Vec<CandidateFailure>,
    pub next_cursor: Option<DueWorkCursor>,
}

#[derive(Clone, Debug)]
pub struct DueWorkScheduler<S, P> {
    store: S,
    planner: P,
    config: DueWorkSchedulerConfig,
}

impl<S, P> DueWorkScheduler<S, P>
where
    S: DueWorkSchedulerStore,
    P: DueWorkPlanner,
{
    pub const fn new(store: S, planner: P, config: DueWorkSchedulerConfig) -> Self {
        Self {
            store,
            planner,
            config,
        }
    }

    /// Scans and independently applies one bounded page of due work.
    ///
    /// A scan failure aborts the tick because there is no authoritative page.
    /// Planning or applying one candidate is recorded in the report and does
    /// not prevent later candidates in the same page from being attempted.
    ///
    /// # Errors
    ///
    /// Returns the Store error from the authoritative candidate scan, or a
    /// constraint error when the configured page size is zero.
    pub async fn tick(
        &self,
        context: &QueryContext,
        after: Option<DueWorkCursor>,
    ) -> Result<DueWorkTickReport, StoreError> {
        if self.config.page_size == 0 {
            return Err(StoreError::new(
                agent_loom_durable_store::StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "Scheduler due-work page size must be positive",
            ));
        }
        let page = self
            .store
            .scan_due_work(
                context,
                DueWorkQuery {
                    after,
                    limit: self.config.page_size,
                },
            )
            .await?;
        let mut report = DueWorkTickReport {
            scanned: page.candidates.len(),
            applied: 0,
            duplicates: 0,
            no_ops: 0,
            failures: Vec::new(),
            next_cursor: page.next_cursor,
        };
        for candidate in page.candidates {
            let planned = match self.planner.plan(candidate) {
                Ok(planned) => planned,
                Err(error) => {
                    report
                        .failures
                        .push(CandidateFailure::Planning { candidate, error });
                    continue;
                }
            };
            match self
                .store
                .apply_due_work(&planned.context, planned.command)
                .await
            {
                Ok(committed) => match committed.disposition {
                    CommandDisposition::Applied => report.applied += 1,
                    CommandDisposition::Duplicate => report.duplicates += 1,
                    CommandDisposition::NoOp => report.no_ops += 1,
                },
                Err(error) => report
                    .failures
                    .push(CandidateFailure::Store { candidate, error }),
            }
        }
        Ok(report)
    }
}

const fn due_work_kind(kind: DueWorkKind) -> &'static str {
    match kind {
        DueWorkKind::ToolRetry => "tool-retry",
        DueWorkKind::AgentRetry => "agent-retry",
    }
}

fn hash(value: &str) -> Digest {
    let bytes: [u8; 32] = Sha256::digest(value.as_bytes()).into();
    Digest::from_bytes(bytes)
}

fn derived_id(namespace: &str, identity: &str) -> [u8; 16] {
    let bytes: [u8; 32] = Sha256::digest(format!("{namespace}/{identity}").as_bytes()).into();
    let mut id = [0; 16];
    id.copy_from_slice(&bytes[..16]);
    id
}

fn hex(bytes: [u8; 16]) -> String {
    let mut value = String::with_capacity(32);
    for byte in bytes {
        use fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn parse_scope(value: &str) -> Result<ScopeKey, DueWorkPlanError> {
    ScopeKey::parse(value).map_err(|_| plan_error("generated Scheduler scope is invalid"))
}

fn parse_idempotency(value: String) -> Result<IdempotencyKey, DueWorkPlanError> {
    IdempotencyKey::parse(value)
        .map_err(|_| plan_error("generated due-work idempotency key is invalid"))
}

fn parse_logical(value: String) -> Result<LogicalKey, DueWorkPlanError> {
    LogicalKey::parse(value)
        .map_err(|_| plan_error("generated recovery Task logical key is invalid"))
}

fn plan_error(message: &str) -> DueWorkPlanError {
    DueWorkPlanError {
        safe_message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agent_loom_domain::{
        AgentExecutionId, RunId, RunStatus, StageExecutionId, TenantId, ToolExecutionId, UnixMicros,
    };
    use agent_loom_durable_store::{CommandDisposition, DueWorkTarget, RetryClass, StoreErrorCode};

    use super::*;

    #[derive(Debug)]
    struct FakeStore {
        page: DueWorkPage,
        seen: Mutex<Vec<ApplyDueWork>>,
        fail_revision: Option<u64>,
    }

    impl DueWorkSchedulerStore for FakeStore {
        fn scan_due_work<'a>(
            &'a self,
            _context: &'a QueryContext,
            _query: DueWorkQuery,
        ) -> StoreFuture<'a, DueWorkPage> {
            Box::pin(async move { Ok(self.page.clone()) })
        }

        fn apply_due_work<'a>(
            &'a self,
            _context: &'a CommandContext,
            command: ApplyDueWork,
        ) -> StoreFuture<'a, Committed<DueWorkOutcome>> {
            Box::pin(async move {
                self.seen
                    .lock()
                    .expect("seen command lock")
                    .push(command.clone());
                if self.fail_revision == Some(command.candidate.expected_revision) {
                    return Err(StoreError::new(
                        StoreErrorCode::VersionConflict,
                        RetryClass::ReloadState,
                        "candidate changed",
                    ));
                }
                Ok(Committed {
                    disposition: CommandDisposition::Applied,
                    value: DueWorkOutcome {
                        tenant_id: command.candidate.tenant_id,
                        run_id: command.candidate.run_id,
                        target: command.candidate.target,
                        recovery_task_id: command.recovery_task.task_id,
                        run_status: RunStatus::Queued,
                        execution_revision: command.candidate.expected_revision,
                        applied_at: command.candidate.due_at,
                    },
                    event_ids: vec![command.applied_event_id],
                    durable_follow_ups: Vec::new(),
                    post_commit_hints: Vec::new(),
                })
            })
        }
    }

    fn candidate(revision: u64, target: DueWorkTarget) -> DueWorkCandidate {
        DueWorkCandidate {
            tenant_id: TenantId::from_bytes([1; 16]),
            run_id: RunId::from_bytes([2; 16]),
            stage_execution_id: Some(StageExecutionId::from_bytes([3; 16])),
            target,
            due_at: UnixMicros::new(100),
            expected_revision: revision,
            run_version: 4,
            execution_generation: 5,
            checkpoint_sequence: 6,
        }
    }

    fn planner() -> DeterministicDueWorkPlanner {
        DeterministicDueWorkPlanner::new("scheduler/test", 10, 3).expect("planner config")
    }

    #[test]
    fn deterministic_planner_preserves_stage_and_replay_identity() {
        let candidate = candidate(7, DueWorkTarget::Tool(ToolExecutionId::from_bytes([8; 16])));
        let first = planner().plan(candidate).expect("first plan");
        let replay = planner().plan(candidate).expect("replayed plan");
        assert_eq!(first, replay);
        assert!(first.command.shape_is_valid());
        assert_eq!(
            first.command.recovery_task.stage_execution_id,
            candidate.stage_execution_id
        );
    }

    #[tokio::test]
    async fn one_candidate_failure_does_not_abort_the_page() {
        let first = candidate(1, DueWorkTarget::Tool(ToolExecutionId::from_bytes([7; 16])));
        let second = candidate(
            2,
            DueWorkTarget::Agent(AgentExecutionId::from_bytes([8; 16])),
        );
        let store = FakeStore {
            page: DueWorkPage {
                candidates: vec![first, second],
                next_cursor: Some(second.cursor()),
            },
            seen: Mutex::new(Vec::new()),
            fail_revision: Some(1),
        };
        let scheduler =
            DueWorkScheduler::new(store, planner(), DueWorkSchedulerConfig { page_size: 2 });
        let report = scheduler
            .tick(
                &QueryContext {
                    tenant_id: first.tenant_id,
                    actor_ref: "scheduler/test".to_owned(),
                    authoritative: true,
                },
                None,
            )
            .await
            .expect("scan succeeds");
        assert_eq!(report.scanned, 2);
        assert_eq!(report.applied, 1);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.next_cursor, Some(second.cursor()));
        assert_eq!(scheduler.store.seen.lock().expect("seen lock").len(), 2);
    }

    #[tokio::test]
    async fn zero_page_size_is_rejected_before_scanning() {
        let store = FakeStore {
            page: DueWorkPage {
                candidates: Vec::new(),
                next_cursor: None,
            },
            seen: Mutex::new(Vec::new()),
            fail_revision: None,
        };
        let scheduler =
            DueWorkScheduler::new(store, planner(), DueWorkSchedulerConfig { page_size: 0 });
        let error = scheduler
            .tick(
                &QueryContext {
                    tenant_id: TenantId::from_bytes([1; 16]),
                    actor_ref: "scheduler/test".to_owned(),
                    authoritative: true,
                },
                None,
            )
            .await
            .expect_err("zero page size must fail");
        assert_eq!(error.code, StoreErrorCode::ConstraintViolation);
    }
}
