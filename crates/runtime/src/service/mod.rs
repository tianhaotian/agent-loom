//! Bounded polling lifecycle for Scheduler and recovery Worker jobs.

use std::{
    error::Error,
    fmt::{self, Write as _},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use agent_loom_domain::{CommandId, Digest, EventId, IdempotencyKey, LeaseToken, WorkerId};
use agent_loom_durable_store::{
    CommandContext, Committed, DueWorkCursor, DurableStore, LeaseReclaimOutcome, QueryContext,
    ReclaimExpiredLease, StoreFuture,
};
use sha2::{Digest as _, Sha256};
use tokio::{sync::watch, task::JoinSet, time::sleep};

use crate::{
    DueWorkPlanner, DueWorkScheduler, DueWorkSchedulerStore, ExternalRecoveryDispatcher,
    RecoveryPollOutcome, RecoveryWorker, RecoveryWorkerStore,
};

const MAX_SERVICE_CONCURRENCY: u32 = 256;

pub type PollingFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PollingActivity, PollingJobError>> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PollingActivity {
    Idle,
    Progress {
        completed: u64,
        failed: u64,
        last_failure: Option<String>,
    },
}

/// One bounded unit of work executed by [`PollingService`].
pub trait PollingJob: Send + Sync {
    fn concurrency_limit(&self) -> u32 {
        MAX_SERVICE_CONCURRENCY
    }

    fn run_once(&self, slot: u32) -> PollingFuture<'_>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PollingJobError {
    pub safe_message: String,
}

impl fmt::Display for PollingJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl Error for PollingJobError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PollingServiceConfig {
    pub concurrency: u32,
    pub busy_delay: Duration,
    pub idle_delay: Duration,
    pub error_delay: Duration,
}

impl Default for PollingServiceConfig {
    fn default() -> Self {
        Self {
            concurrency: 1,
            busy_delay: Duration::from_millis(10),
            idle_delay: Duration::from_secs(1),
            error_delay: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PollingServiceConfigError {
    pub safe_message: String,
}

impl fmt::Display for PollingServiceConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl Error for PollingServiceConfigError {}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PollingServiceReport {
    pub batches: u64,
    pub attempts: u64,
    pub idle: u64,
    pub completed: u64,
    pub failed: u64,
    pub infrastructure_failures: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PollingService<J> {
    job: Arc<J>,
    config: PollingServiceConfig,
}

impl<J> PollingService<J>
where
    J: PollingJob + 'static,
{
    /// Creates a bounded polling service.
    ///
    /// # Errors
    ///
    /// Returns an error for zero/excessive concurrency, zero delays, or a
    /// concurrency setting that exceeds the Job's declared safety limit.
    pub fn new(job: J, config: PollingServiceConfig) -> Result<Self, PollingServiceConfigError> {
        validate_service_config(&config, job.concurrency_limit())?;
        Ok(Self {
            job: Arc::new(job),
            config,
        })
    }

    /// Runs batches until shutdown is requested. Shutdown is graceful: an
    /// already-started bounded batch finishes before this method returns.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> PollingServiceReport {
        let mut report = PollingServiceReport::default();
        while !*shutdown.borrow() {
            let mut jobs = JoinSet::new();
            for slot in 0..self.config.concurrency {
                let job = Arc::clone(&self.job);
                jobs.spawn(async move { job.run_once(slot).await });
            }

            report.batches += 1;
            let failures_before = report.infrastructure_failures;
            let progress_before = report.completed + report.failed;
            while let Some(result) = jobs.join_next().await {
                report.attempts += 1;
                match result {
                    Ok(Ok(PollingActivity::Idle)) => report.idle += 1,
                    Ok(Ok(PollingActivity::Progress {
                        completed,
                        failed,
                        last_failure,
                    })) => {
                        report.completed += completed;
                        report.failed += failed;
                        if last_failure.is_some() {
                            report.last_error = last_failure;
                        }
                    }
                    Ok(Err(error)) => {
                        report.infrastructure_failures += 1;
                        report.last_error = Some(error.safe_message);
                    }
                    Err(_) => {
                        report.infrastructure_failures += 1;
                        report.last_error = Some("Polling Job terminated unexpectedly".to_owned());
                    }
                }
            }

            if *shutdown.borrow() {
                break;
            }
            let delay = if report.infrastructure_failures > failures_before {
                self.config.error_delay
            } else if report.completed + report.failed > progress_before {
                self.config.busy_delay
            } else {
                self.config.idle_delay
            };
            tokio::select! {
                () = sleep(delay) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        report
    }
}

#[derive(Clone, Debug)]
pub struct PollingShutdownHandle {
    sender: watch::Sender<bool>,
}

impl PollingShutdownHandle {
    pub fn request_shutdown(&self) -> bool {
        self.sender.send(true).is_ok()
    }
}

pub fn polling_shutdown_channel() -> (PollingShutdownHandle, watch::Receiver<bool>) {
    let (sender, receiver) = watch::channel(false);
    (PollingShutdownHandle { sender }, receiver)
}

#[derive(Debug)]
pub struct DueWorkPollingJob<S, P> {
    scheduler: DueWorkScheduler<S, P>,
    context: QueryContext,
    cursor: Mutex<Option<DueWorkCursor>>,
}

impl<S, P> DueWorkPollingJob<S, P> {
    pub const fn new(scheduler: DueWorkScheduler<S, P>, context: QueryContext) -> Self {
        Self {
            scheduler,
            context,
            cursor: Mutex::new(None),
        }
    }

    /// Returns the next keyset cursor.
    ///
    /// # Errors
    ///
    /// Returns a safe error if another thread poisoned the cursor lock.
    pub fn cursor(&self) -> Result<Option<DueWorkCursor>, PollingJobError> {
        self.cursor
            .lock()
            .map(|cursor| *cursor)
            .map_err(|_| polling_error("due-work cursor lock is unavailable"))
    }
}

impl<S, P> PollingJob for DueWorkPollingJob<S, P>
where
    S: DueWorkSchedulerStore,
    P: DueWorkPlanner,
{
    fn concurrency_limit(&self) -> u32 {
        1
    }

    fn run_once(&self, _slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            let after = self
                .cursor
                .lock()
                .map_err(|_| polling_error("due-work cursor lock is unavailable"))?
                .to_owned();
            let report = self
                .scheduler
                .tick(&self.context, after)
                .await
                .map_err(|error| polling_error(&error.to_string()))?;
            *self
                .cursor
                .lock()
                .map_err(|_| polling_error("due-work cursor lock is unavailable"))? =
                report.next_cursor;
            if report.scanned == 0 {
                return Ok(PollingActivity::Idle);
            }
            let last_failure = report.failures.last().map(candidate_failure_message);
            Ok(PollingActivity::Progress {
                completed: usize_to_u64(report.applied + report.duplicates + report.no_ops)?,
                failed: usize_to_u64(report.failures.len())?,
                last_failure,
            })
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryLeaseIdentity {
    pub worker_id: WorkerId,
    pub lease_token: LeaseToken,
}

pub trait RecoveryIdentitySource: Send + Sync {
    /// Creates a Worker/Lease identity for one poll attempt.
    ///
    /// # Errors
    ///
    /// Returns a safe error when a collision-resistant Lease token cannot be generated.
    fn next(&self, slot: u32) -> Result<RecoveryLeaseIdentity, PollingJobError>;
}

/// Minimal Store surface used by the expired-Lease reclaimer.
pub trait LeaseReclaimStore: Send + Sync {
    fn reclaim_expired_lease<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ReclaimExpiredLease,
    ) -> StoreFuture<'a, Option<Committed<LeaseReclaimOutcome>>>;
}

impl<T> LeaseReclaimStore for T
where
    T: DurableStore + ?Sized,
{
    fn reclaim_expired_lease<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ReclaimExpiredLease,
    ) -> StoreFuture<'a, Option<Committed<LeaseReclaimOutcome>>> {
        DurableStore::reclaim_expired_lease(self, context, command)
    }
}

pub struct SeededRecoveryIdentitySource {
    worker_id: WorkerId,
    process_seed: LeaseToken,
    sequence: AtomicU64,
}

impl fmt::Debug for SeededRecoveryIdentitySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SeededRecoveryIdentitySource")
            .field("worker_id", &self.worker_id)
            .field("process_seed", &"[REDACTED]")
            .field("sequence", &self.sequence.load(Ordering::Relaxed))
            .finish()
    }
}

impl SeededRecoveryIdentitySource {
    /// Creates an identity source from a process-unique Worker ID and secret seed.
    /// A new random seed must be supplied on every process start.
    ///
    /// # Errors
    ///
    /// Returns an error when the Worker ID is nil or the seed is all zeroes.
    pub fn new(worker_id: WorkerId, process_seed: LeaseToken) -> Result<Self, PollingJobError> {
        if worker_id.is_nil() || process_seed.as_bytes() == &[0; 32] {
            return Err(polling_error(
                "Recovery identity requires a non-nil Worker and nonzero process seed",
            ));
        }
        Ok(Self {
            worker_id,
            process_seed,
            sequence: AtomicU64::new(0),
        })
    }
}

impl RecoveryIdentitySource for SeededRecoveryIdentitySource {
    fn next(&self, slot: u32) -> Result<RecoveryLeaseIdentity, PollingJobError> {
        let sequence = self
            .sequence
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| polling_error("Recovery Lease identity sequence exhausted"))?;
        let mut hasher = Sha256::new();
        hasher.update(self.process_seed.as_bytes());
        hasher.update(self.worker_id.as_bytes());
        hasher.update(slot.to_be_bytes());
        hasher.update(sequence.to_be_bytes());
        Ok(RecoveryLeaseIdentity {
            worker_id: self.worker_id,
            lease_token: LeaseToken::from_bytes(hasher.finalize().into()),
        })
    }
}

#[derive(Debug)]
pub struct RecoveryPollingJob<S, D, I> {
    worker: RecoveryWorker<S, D>,
    context_template: CommandContext,
    identities: I,
}

impl<S, D, I> RecoveryPollingJob<S, D, I> {
    pub const fn new(
        worker: RecoveryWorker<S, D>,
        context_template: CommandContext,
        identities: I,
    ) -> Self {
        Self {
            worker,
            context_template,
            identities,
        }
    }
}

impl<S, D, I> PollingJob for RecoveryPollingJob<S, D, I>
where
    S: RecoveryWorkerStore,
    D: ExternalRecoveryDispatcher,
    I: RecoveryIdentitySource,
{
    fn run_once(&self, slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            let identity = self.identities.next(slot)?;
            let claim_context = recovery_claim_context(&self.context_template, &identity)?;
            match self
                .worker
                .poll_once(&claim_context, identity.worker_id, identity.lease_token)
                .await
                .map_err(|error| polling_error(&error.to_string()))?
            {
                RecoveryPollOutcome::Idle => Ok(PollingActivity::Idle),
                RecoveryPollOutcome::Dispatched { .. } => Ok(PollingActivity::Progress {
                    completed: 1,
                    failed: 0,
                    last_failure: None,
                }),
                RecoveryPollOutcome::DispatchFailed { error, .. } => {
                    Ok(PollingActivity::Progress {
                        completed: 0,
                        failed: 1,
                        last_failure: Some(error.safe_message),
                    })
                }
            }
        })
    }
}

#[derive(Debug)]
pub struct LeaseReclaimPollingJob<S, I> {
    store: S,
    context_template: CommandContext,
    identities: I,
}

impl<S, I> LeaseReclaimPollingJob<S, I> {
    pub const fn new(store: S, context_template: CommandContext, identities: I) -> Self {
        Self {
            store,
            context_template,
            identities,
        }
    }
}

impl<S, I> PollingJob for LeaseReclaimPollingJob<S, I>
where
    S: LeaseReclaimStore,
    I: RecoveryIdentitySource,
{
    fn run_once(&self, slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            let identity = self.identities.next(slot)?;
            let context =
                polling_command_context(&self.context_template, &identity, "lease-reclaim")?;
            let outcome = self
                .store
                .reclaim_expired_lease(
                    &context,
                    ReclaimExpiredLease {
                        reclaimed_event_id: EventId::from_bytes(context.command_id.into_bytes()),
                    },
                )
                .await
                .map_err(|error| polling_error(&error.to_string()))?;
            Ok(if outcome.is_some() {
                PollingActivity::Progress {
                    completed: 1,
                    failed: 0,
                    last_failure: None,
                }
            } else {
                PollingActivity::Idle
            })
        })
    }
}

fn recovery_claim_context(
    template: &CommandContext,
    identity: &RecoveryLeaseIdentity,
) -> Result<CommandContext, PollingJobError> {
    polling_command_context(template, identity, "recovery-claim")
}

fn polling_command_context(
    template: &CommandContext,
    identity: &RecoveryLeaseIdentity,
    operation: &str,
) -> Result<CommandContext, PollingJobError> {
    let token_digest: [u8; 32] = Sha256::digest(identity.lease_token.as_bytes()).into();
    let mut token_hex = String::with_capacity(64);
    for byte in token_digest {
        write!(token_hex, "{byte:02x}")
            .map_err(|_| polling_error("Recovery claim identity could not be formatted"))?;
    }
    let logical_identity = format!("{operation}/{}/{token_hex}", identity.worker_id);
    let command_digest: [u8; 32] = Sha256::digest(logical_identity.as_bytes()).into();
    let mut command_id = [0; 16];
    command_id.copy_from_slice(&command_digest[..16]);
    Ok(CommandContext {
        tenant_id: template.tenant_id,
        command_id: CommandId::from_bytes(command_id),
        correlation_id: template.correlation_id,
        actor_ref: template.actor_ref.clone(),
        scope: template.scope.clone(),
        idempotency_key: IdempotencyKey::parse(logical_identity)
            .map_err(|_| polling_error("Recovery claim idempotency key is invalid"))?,
        request_hash: Digest::from_bytes(command_digest),
    })
}

fn validate_service_config(
    config: &PollingServiceConfig,
    job_limit: u32,
) -> Result<(), PollingServiceConfigError> {
    if config.concurrency == 0
        || config.concurrency > MAX_SERVICE_CONCURRENCY
        || config.concurrency > job_limit
    {
        return Err(service_config_error(
            "Polling concurrency is zero or exceeds the safe Job limit",
        ));
    }
    if config.busy_delay.is_zero() || config.idle_delay.is_zero() || config.error_delay.is_zero() {
        return Err(service_config_error(
            "Polling busy, idle, and error delays must be positive",
        ));
    }
    Ok(())
}

fn candidate_failure_message(failure: &crate::CandidateFailure) -> String {
    match failure {
        crate::CandidateFailure::Planning { error, .. } => error.safe_message.clone(),
        crate::CandidateFailure::Store { error, .. } => error.to_string(),
    }
}

fn usize_to_u64(value: usize) -> Result<u64, PollingJobError> {
    u64::try_from(value).map_err(|_| polling_error("Polling counter exceeds u64 range"))
}

fn polling_error(message: &str) -> PollingJobError {
    PollingJobError {
        safe_message: message.to_owned(),
    }
}

fn service_config_error(message: &str) -> PollingServiceConfigError {
    PollingServiceConfigError {
        safe_message: message.to_owned(),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
