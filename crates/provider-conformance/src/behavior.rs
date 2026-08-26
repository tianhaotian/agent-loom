use std::{error::Error, fmt, time::Duration};

use agent_loom_domain::{
    CheckpointId, CommandId, CorrelationId, Digest, DurationMicros, EventId, IdempotencyKey,
    JsonPayload, LeaseToken, LogicalKey, RunId, RunStatus, ScopeKey, TaskId, TaskKind, TenantId,
    UnixMicros, WorkerId,
};
use agent_loom_durable_store::{
    ClaimTask, CommandContext, CommandDisposition, ControlRun, CreateRun, DurableStore,
    ExpectedRun, InitialTask, LeaseExpiryAction, LeaseProof, NewCheckpoint, QueryContext,
    ReclaimExpiredLease, RenewTaskLease, StoreError, conformance::ConformanceCase,
};

const CASE: ConformanceCase = ConformanceCase::LeaseExpiryRetry;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseExpiryRetryFixture {
    pub tenant_id: TenantId,
    pub identity_seed: [u8; 16],
    pub actor_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseExpiryRetryReport {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub reclaimed_event_id: EventId,
    pub first_attempt: u32,
    pub retry_attempt: u32,
    pub final_status: RunStatus,
}

#[derive(Debug)]
pub enum BehaviorConformanceError {
    Store(StoreError),
    Invariant {
        case: ConformanceCase,
        detail: &'static str,
    },
}

impl fmt::Display for BehaviorConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "{CASE:?} Store operation failed: {error}"),
            Self::Invariant { case, detail } => {
                write!(formatter, "{case:?} invariant failed: {detail}")
            }
        }
    }
}

impl Error for BehaviorConformanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Invariant { .. } => None,
        }
    }
}

impl From<StoreError> for BehaviorConformanceError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// Exercises the portable expired-Lease behavior through `dyn DurableStore` only.
///
/// The caller provisions the fixture Tenant. The scenario creates a Run, claims a
/// short Lease, renews it, verifies receipt replay, waits for database-authoritative
/// expiry, reclaims it, claims attempt two, and cancels the Run to close the fixture.
///
/// # Errors
///
/// Returns a Store failure or a provider behavior mismatch.
#[allow(clippy::too_many_lines)]
pub async fn exercise_lease_expiry_retry(
    store: &dyn DurableStore,
    fixture: LeaseExpiryRetryFixture,
) -> Result<LeaseExpiryRetryReport, BehaviorConformanceError> {
    let run_id = RunId::from_bytes(derived_identity(fixture.identity_seed, 1));
    let task_id = TaskId::from_bytes(derived_identity(fixture.identity_seed, 2));
    let initial_event_id = EventId::from_bytes(derived_identity(fixture.identity_seed, 3));
    let checkpoint_id = CheckpointId::from_bytes(derived_identity(fixture.identity_seed, 4));
    let create_context = command_context(&fixture, run_id, "create_run", 5)?;
    let empty_payload = || JsonPayload::from_validated_bytes(b"{}".to_vec());
    let created = store
        .create_run(
            &create_context,
            CreateRun {
                run_id,
                workflow_version_id: None,
                coordinator_agent_version_id: None,
                input: empty_payload(),
                deadline: None,
                initial_event_id,
                initial_checkpoint: NewCheckpoint {
                    checkpoint_id,
                    sequence: 1,
                    schema_version: 1,
                    workflow_version_id: None,
                    coordinator_agent_version_id: None,
                    execution_generation: 0,
                    state: empty_payload(),
                    state_digest: Digest::from_bytes([4; 32]),
                    created_event_id: initial_event_id,
                },
                initial_stages: Vec::new(),
                initial_tasks: vec![InitialTask {
                    task_id,
                    stage_execution_id: None,
                    logical_key: LogicalKey::parse(format!("conformance/{run_id}/lease"))
                        .map_err(|_| mismatch("generated Task logical key is invalid"))?,
                    kind: TaskKind::Model,
                    priority: 10,
                    available_at: UnixMicros::new(0),
                    max_attempts: 2,
                    input: empty_payload(),
                }],
            },
        )
        .await?;
    require(
        created.disposition == CommandDisposition::Applied
            && created.value.status == RunStatus::Queued,
        "Run creation did not produce a new queued Run",
    )?;

    let first_worker = WorkerId::from_bytes(derived_identity(fixture.identity_seed, 6));
    let first_token = LeaseToken::from_bytes([6; 32]);
    let first_claim_context = command_context(&fixture, run_id, "claim_task", 7)?;
    let first_claim = store
        .claim_task(
            &first_claim_context,
            ClaimTask {
                worker_id: first_worker,
                lease_token: first_token.clone(),
                lease_duration: DurationMicros::new(1_000_000),
                candidate_window: 8,
                kind: None,
            },
        )
        .await?
        .ok_or(BehaviorConformanceError::Invariant {
            case: CASE,
            detail: "initial Task was not claimable",
        })?;
    require(
        first_claim.value.task.task_id == task_id && first_claim.value.task.attempt == 1,
        "initial claim did not create attempt one",
    )?;

    let renew_context = command_context(&fixture, run_id, "renew_task_lease", 8)?;
    let renew_command = RenewTaskLease {
        expected_run: ExpectedRun {
            run_id,
            version: Some(first_claim.value.run_version),
            execution_generation: Some(first_claim.value.task.generation),
        },
        lease: LeaseProof {
            task_id,
            worker_id: first_worker,
            token: first_token,
            execution_generation: first_claim.value.task.generation,
        },
        extension: DurationMicros::new(1_000_000),
    };
    let renewed = store
        .renew_task_lease(&renew_context, renew_command.clone())
        .await?;
    require(
        renewed.disposition == CommandDisposition::Applied
            && renewed.value.lease_expires_at > first_claim.value.lease_expires_at,
        "Lease renewal did not extend the authoritative expiry",
    )?;
    let renew_duplicate = store
        .renew_task_lease(&renew_context, renew_command)
        .await?;
    require(
        renew_duplicate.disposition == CommandDisposition::Duplicate
            && renew_duplicate.value == renewed.value,
        "Lease renewal receipt replay changed the committed outcome",
    )?;

    tokio::time::sleep(Duration::from_millis(2_200)).await;

    let reclaim_bytes = derived_identity(fixture.identity_seed, 9);
    let reclaim_context = command_context(&fixture, run_id, "reclaim_expired_lease", 9)?;
    let reclaim_command = ReclaimExpiredLease {
        reclaimed_event_id: EventId::from_bytes(reclaim_bytes),
    };
    let reclaimed = store
        .reclaim_expired_lease(&reclaim_context, reclaim_command)
        .await?
        .ok_or(BehaviorConformanceError::Invariant {
            case: CASE,
            detail: "expired Lease was not reclaimed",
        })?;
    require(
        reclaimed.disposition == CommandDisposition::Applied
            && reclaimed.value.run_id == run_id
            && reclaimed.value.task_id == task_id
            && reclaimed.value.attempt == 1
            && reclaimed.value.action == LeaseExpiryAction::RetryScheduled,
        "Lease reclaim did not schedule attempt one for retry",
    )?;
    let reclaim_duplicate = store
        .reclaim_expired_lease(&reclaim_context, reclaim_command)
        .await?
        .ok_or(BehaviorConformanceError::Invariant {
            case: CASE,
            detail: "Lease reclaim receipt replay returned no outcome",
        })?;
    require(
        reclaim_duplicate.disposition == CommandDisposition::Duplicate
            && reclaim_duplicate.value == reclaimed.value
            && reclaim_duplicate.event_ids == reclaimed.event_ids,
        "Lease reclaim receipt replay changed the committed outcome",
    )?;

    let query_context = QueryContext {
        tenant_id: fixture.tenant_id,
        actor_ref: fixture.actor_ref.clone(),
        authoritative: true,
    };
    let retrying = store.get_run(&query_context, run_id).await?.ok_or(
        BehaviorConformanceError::Invariant {
            case: CASE,
            detail: "Run disappeared after Lease reclaim",
        },
    )?;
    require(
        retrying.status == RunStatus::Retrying,
        "Lease reclaim did not project the Run to retrying",
    )?;

    let retry_worker = WorkerId::from_bytes(derived_identity(fixture.identity_seed, 10));
    let retry_claim_context = command_context(&fixture, run_id, "claim_task", 11)?;
    let retry_claim = store
        .claim_task(
            &retry_claim_context,
            ClaimTask {
                worker_id: retry_worker,
                lease_token: LeaseToken::from_bytes([11; 32]),
                lease_duration: DurationMicros::new(60_000_000),
                candidate_window: 8,
                kind: None,
            },
        )
        .await?
        .ok_or(BehaviorConformanceError::Invariant {
            case: CASE,
            detail: "retry_scheduled Task was not claimable from retrying Run",
        })?;
    require(
        retry_claim.value.task.task_id == task_id && retry_claim.value.task.attempt == 2,
        "retry claim did not create attempt two",
    )?;

    let running = store.get_run(&query_context, run_id).await?.ok_or(
        BehaviorConformanceError::Invariant {
            case: CASE,
            detail: "Run disappeared after retry claim",
        },
    )?;
    require(
        running.status == RunStatus::Running,
        "retry claim did not project the Run to running",
    )?;
    let cancel_bytes = derived_identity(fixture.identity_seed, 12);
    let cancel_context = command_context(&fixture, run_id, "cancel_run", 12)?;
    let cancelled = store
        .cancel_run(
            &cancel_context,
            ControlRun {
                expected_run: ExpectedRun {
                    run_id,
                    version: Some(running.version),
                    execution_generation: Some(running.execution_generation),
                },
                event_id: EventId::from_bytes(cancel_bytes),
                reason: "close provider conformance fixture".to_owned(),
            },
        )
        .await?;
    require(
        cancelled.value.status == RunStatus::Cancelled
            && cancelled.value.terminal_invariant_holds(),
        "fixture cleanup did not produce a valid cancelled terminal Run",
    )?;

    Ok(LeaseExpiryRetryReport {
        run_id,
        task_id,
        reclaimed_event_id: reclaim_command.reclaimed_event_id,
        first_attempt: first_claim.value.task.attempt,
        retry_attempt: retry_claim.value.task.attempt,
        final_status: cancelled.value.status,
    })
}

fn command_context(
    fixture: &LeaseExpiryRetryFixture,
    run_id: RunId,
    scope: &str,
    tag: u16,
) -> Result<CommandContext, BehaviorConformanceError> {
    let identity = derived_identity(fixture.identity_seed, tag);
    Ok(CommandContext {
        tenant_id: fixture.tenant_id,
        command_id: CommandId::from_bytes(identity),
        correlation_id: CorrelationId::from_bytes(identity),
        actor_ref: fixture.actor_ref.clone(),
        scope: ScopeKey::parse(scope).map_err(|_| mismatch("conformance scope is invalid"))?,
        idempotency_key: IdempotencyKey::parse(format!("{scope}-{run_id}-{tag}"))
            .map_err(|_| mismatch("generated idempotency key is invalid"))?,
        request_hash: Digest::from_bytes(
            [u8::try_from(tag).map_err(|_| mismatch("fixture identity tag is too large"))?; 32],
        ),
    })
}

fn derived_identity(mut seed: [u8; 16], tag: u16) -> [u8; 16] {
    let suffix = u16::from_be_bytes([seed[14], seed[15]]).wrapping_add(tag);
    let [high, low] = suffix.to_be_bytes();
    seed[14] = high;
    seed[15] = low;
    seed
}

fn require(condition: bool, detail: &'static str) -> Result<(), BehaviorConformanceError> {
    if condition {
        Ok(())
    } else {
        Err(mismatch(detail))
    }
}

const fn mismatch(detail: &'static str) -> BehaviorConformanceError {
    BehaviorConformanceError::Invariant { case: CASE, detail }
}
