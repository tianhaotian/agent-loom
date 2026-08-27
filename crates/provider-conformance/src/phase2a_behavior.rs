use std::{error::Error, fmt};

use agent_loom_domain::{
    CheckpointId, CommandId, CorrelationId, Digest, DurationMicros, EventId, IdempotencyKey,
    JsonPayload, LeaseToken, LogicalKey, RunId, RunStatus, ScopeKey, TaskId, TaskKind, TenantId,
    UnixMicros, WaitStatus, WorkerId,
};
use agent_loom_durable_store::{
    ApplyEvent, ClaimTask, ClaimedTask, CommandContext, CompleteTask, ControlRun, CreateRun,
    DurableStore, EventCursor, ExpectedRun, FinalRunResult, InitialTask, LeaseProof, NewCheckpoint,
    NewWaitSubscription, NextActions, QueryContext, SignatureVerification, StoreError, TaskResult,
    WaitResumeTask, conformance::ConformanceCase,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Phase2aReliabilityFixture {
    pub tenant_id: TenantId,
    pub identity_seed: [u8; 16],
    pub actor_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Phase2aReliabilityReport {
    pub concurrent_claim_winners: usize,
    pub terminal_race_status: RunStatus,
    pub consumed_wait_events: usize,
    pub atomic_rollback_preserved_version: bool,
}

#[derive(Debug)]
pub enum Phase2aReliabilityError {
    Store(StoreError),
    Invariant {
        case: ConformanceCase,
        detail: &'static str,
    },
}

impl fmt::Display for Phase2aReliabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "Phase 2A Store operation failed: {error}"),
            Self::Invariant { case, detail } => {
                write!(formatter, "{case:?} invariant failed: {detail}")
            }
        }
    }
}

impl Error for Phase2aReliabilityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Invariant { .. } => None,
        }
    }
}

impl From<StoreError> for Phase2aReliabilityError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

/// Runs the Phase 2A concurrent-claim, terminal-race, Wait-consumption and
/// transaction-rollback cases through the provider-neutral `DurableStore` contract.
///
/// # Errors
///
/// Returns a Store failure or the first portable behavior mismatch.
pub async fn exercise_phase2a_reliability(
    store: &dyn DurableStore,
    fixture: Phase2aReliabilityFixture,
) -> Result<Phase2aReliabilityReport, Phase2aReliabilityError> {
    let concurrent_claim_winners = concurrent_claim_case(store, &fixture, 100).await?;
    let terminal_race_status = terminal_race_case(store, &fixture, 200).await?;
    let consumed_wait_events = wait_consumption_case(store, &fixture, 300).await?;
    let atomic_rollback_preserved_version = completion_atomicity_case(store, &fixture, 400).await?;
    Ok(Phase2aReliabilityReport {
        concurrent_claim_winners,
        terminal_race_status,
        consumed_wait_events,
        atomic_rollback_preserved_version,
    })
}

async fn concurrent_claim_case(
    store: &dyn DurableStore,
    fixture: &Phase2aReliabilityFixture,
    base: u16,
) -> Result<usize, Phase2aReliabilityError> {
    let case = ConformanceCase::ConcurrentTaskClaim;
    let created = create_task_run(store, fixture, base).await?;
    let first_context = context(fixture, created.run_id, "concurrent_claim_a", base + 10)?;
    let second_context = context(fixture, created.run_id, "concurrent_claim_b", base + 11)?;
    let first_command = claim_command(fixture, base + 12);
    let second_command = claim_command(fixture, base + 13);
    let (first, second) = tokio::join!(
        store.claim_task(&first_context, first_command),
        store.claim_task(&second_context, second_command),
    );
    let first = first?;
    let second = second?;
    let winners = usize::from(first.is_some()) + usize::from(second.is_some());
    require(
        winners == 1,
        case,
        "two Workers did not produce exactly one authoritative Task claim",
    )?;
    cancel_fixture(store, fixture, created.run_id, base + 14).await?;
    Ok(winners)
}

async fn terminal_race_case(
    store: &dyn DurableStore,
    fixture: &Phase2aReliabilityFixture,
    base: u16,
) -> Result<RunStatus, Phase2aReliabilityError> {
    let case = ConformanceCase::UniqueTerminalEvent;
    let created = create_task_run(store, fixture, base).await?;
    let claim_context = context(fixture, created.run_id, "terminal_claim", base + 10)?;
    let claim_command = claim_command(fixture, base + 11);
    let claim = store
        .claim_task(&claim_context, claim_command.clone())
        .await?
        .ok_or_else(|| invariant(case, "terminal-race Task was not claimable"))?;
    let completion_event_id = event_id(fixture, base + 12);
    let complete_context = context(fixture, created.run_id, "complete_task", base + 13)?;
    let cancel_context = context(fixture, created.run_id, "cancel_run", base + 14)?;
    let complete = completion(
        fixture,
        &created,
        &claim.value,
        lease_proof(created.task_id, &claim_command, claim.value.task.generation),
        completion_event_id,
        checkpoint_id(fixture, base + 15),
        NextActions::FinishRun(FinalRunResult {
            status: RunStatus::Completed,
            output: empty_payload(),
        }),
    );
    let cancel = ControlRun {
        expected_run: expected_claim_run(created.run_id, &claim.value),
        event_id: event_id(fixture, base + 16),
        reason: "race terminal completion".to_owned(),
    };
    let (completed, cancelled) = tokio::join!(
        store.complete_task(&complete_context, complete),
        store.cancel_run(&cancel_context, cancel),
    );
    require(
        completed.is_ok() || cancelled.is_ok(),
        case,
        "both terminal contenders failed without committing a winner",
    )?;
    let query = query_context(fixture);
    let terminal = store
        .get_run(&query, created.run_id)
        .await?
        .ok_or_else(|| invariant(case, "terminal-race Run disappeared"))?;
    require(
        terminal.terminal_invariant_holds(),
        case,
        "terminal race produced an invalid terminal projection",
    )?;
    let events = store
        .list_events(
            &query,
            EventCursor {
                run_id: created.run_id,
                after_sequence: 0,
                limit: 100,
            },
        )
        .await?;
    let terminal_event_id = terminal
        .terminal_event_id
        .ok_or_else(|| invariant(case, "terminal Run omitted terminal Event ownership"))?;
    require(
        events
            .events
            .iter()
            .filter(|event| event.event_id == terminal_event_id)
            .count()
            == 1,
        case,
        "terminal race did not retain exactly one authoritative terminal Event",
    )?;
    Ok(terminal.status)
}

#[allow(clippy::too_many_lines)]
async fn wait_consumption_case(
    store: &dyn DurableStore,
    fixture: &Phase2aReliabilityFixture,
    base: u16,
) -> Result<usize, Phase2aReliabilityError> {
    let case = ConformanceCase::WaitSingleConsumption;
    let created = create_task_run(store, fixture, base).await?;
    let claim_context = context(fixture, created.run_id, "wait_claim", base + 10)?;
    let claim_command = claim_command(fixture, base + 11);
    let claim = store
        .claim_task(&claim_context, claim_command.clone())
        .await?
        .ok_or_else(|| invariant(case, "Wait fixture Task was not claimable"))?;
    let wait_event_id = event_id(fixture, base + 12);
    let match_key_hash = Digest::from_bytes([31; 32]);
    let wait_context = context(fixture, created.run_id, "create_wait", base + 13)?;
    let waiting = store
        .complete_task(
            &wait_context,
            completion(
                fixture,
                &created,
                &claim.value,
                lease_proof(created.task_id, &claim_command, claim.value.task.generation),
                wait_event_id,
                checkpoint_id(fixture, base + 14),
                NextActions::Wait(NewWaitSubscription {
                    wait_id: agent_loom_domain::WaitId::from_bytes(identity(fixture, base + 15)),
                    stage_execution_id: None,
                    wait_type: "approval".to_owned(),
                    expected_event_type: "approval.granted".to_owned(),
                    match_key_hash,
                    match_contract: empty_payload(),
                    expires_at: None,
                    resume_task: WaitResumeTask {
                        task_id: task_id(fixture, base + 16),
                        logical_key: LogicalKey::parse(format!(
                            "conformance/{}/approval-resume",
                            created.run_id
                        ))
                        .map_err(|_| invariant(case, "Wait resume logical key is invalid"))?,
                        kind: TaskKind::Model,
                        priority: 10,
                        max_attempts: 2,
                        input: empty_payload(),
                        deadline: None,
                    },
                    created_event_id: wait_event_id,
                }),
            ),
        )
        .await?;
    require(
        waiting.value.status == RunStatus::ApprovalRequired,
        case,
        "approval Wait did not project the Run to approval_required",
    )?;

    let first_context = context(fixture, created.run_id, "approval_event_a", base + 17)?;
    let second_context = context(fixture, created.run_id, "approval_event_b", base + 18)?;
    let event = |tag| ApplyEvent {
        expected_run: ExpectedRun {
            run_id: created.run_id,
            version: Some(waiting.value.version),
            execution_generation: Some(waiting.value.execution_generation),
        },
        event_id: event_id(fixture, tag),
        event_type: "approval.granted".to_owned(),
        match_key_hash,
        payload_schema_version: 1,
        payload: empty_payload(),
        signature_verification: SignatureVerification::Verified,
        occurred_at: None,
    };
    let (first, second) = tokio::join!(
        store.apply_event(&first_context, event(base + 19)),
        store.apply_event(&second_context, event(base + 20)),
    );
    require(
        first.is_ok() || second.is_ok(),
        case,
        "both matching approval Events failed without consuming the Wait",
    )?;
    let query = query_context(fixture);
    let waits = store.list_waits(&query, created.run_id).await?;
    require(
        waits.len() == 1 && waits[0].status == WaitStatus::Consumed,
        case,
        "matching approval Events did not consume exactly one Wait",
    )?;
    let events = store
        .list_events(
            &query,
            EventCursor {
                run_id: created.run_id,
                after_sequence: 0,
                limit: 100,
            },
        )
        .await?;
    let consumed = events
        .events
        .iter()
        .filter(|event| event.event_type == "approval.granted")
        .count();
    require(
        consumed == 1,
        case,
        "approval race persisted more than one consuming Event",
    )?;
    cancel_fixture(store, fixture, created.run_id, base + 21).await?;
    Ok(consumed)
}

async fn completion_atomicity_case(
    store: &dyn DurableStore,
    fixture: &Phase2aReliabilityFixture,
    base: u16,
) -> Result<bool, Phase2aReliabilityError> {
    let case = ConformanceCase::TaskCompletionAtomicity;
    let created = create_task_run(store, fixture, base).await?;
    let claim_context = context(fixture, created.run_id, "atomic_claim", base + 10)?;
    let claim_command = claim_command(fixture, base + 11);
    let claim = store
        .claim_task(&claim_context, claim_command.clone())
        .await?
        .ok_or_else(|| invariant(case, "atomicity fixture Task was not claimable"))?;
    let query = query_context(fixture);
    let before = store
        .list_events(
            &query,
            EventCursor {
                run_id: created.run_id,
                after_sequence: 0,
                limit: 100,
            },
        )
        .await?;
    let failure_context = context(fixture, created.run_id, "atomic_completion", base + 12)?;
    let failure = store
        .complete_task(
            &failure_context,
            completion(
                fixture,
                &created,
                &claim.value,
                lease_proof(created.task_id, &claim_command, claim.value.task.generation),
                event_id(fixture, base + 13),
                created.initial_checkpoint_id,
                NextActions::NoFurtherWork,
            ),
        )
        .await;
    require(
        failure.is_err(),
        case,
        "duplicate Checkpoint fault injection unexpectedly committed",
    )?;
    let after_run = store
        .get_run(&query, created.run_id)
        .await?
        .ok_or_else(|| invariant(case, "atomicity fixture Run disappeared"))?;
    let after_events = store
        .list_events(
            &query,
            EventCursor {
                run_id: created.run_id,
                after_sequence: 0,
                limit: 100,
            },
        )
        .await?;
    let preserved = after_run.version == claim.value.run_version
        && after_run.status == RunStatus::Running
        && after_events.events == before.events;
    require(
        preserved,
        case,
        "failed completion left a partial Event, Checkpoint, Task, or Run update",
    )?;
    cancel_fixture(store, fixture, created.run_id, base + 14).await?;
    Ok(preserved)
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
struct CreatedTaskRun {
    run_id: RunId,
    task_id: TaskId,
    initial_checkpoint_id: CheckpointId,
}

async fn create_task_run(
    store: &dyn DurableStore,
    fixture: &Phase2aReliabilityFixture,
    base: u16,
) -> Result<CreatedTaskRun, Phase2aReliabilityError> {
    let run_id = RunId::from_bytes(identity(fixture, base + 1));
    let task_id = task_id(fixture, base + 2);
    let initial_event_id = event_id(fixture, base + 3);
    let initial_checkpoint_id = checkpoint_id(fixture, base + 4);
    let create_context = context(fixture, run_id, "phase2a_create_run", base + 5)?;
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
                    checkpoint_id: initial_checkpoint_id,
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
                    logical_key: LogicalKey::parse(format!("conformance/{run_id}/phase2a"))
                        .map_err(|_| {
                            invariant(
                                ConformanceCase::CreateRunAtomicity,
                                "generated Task logical key is invalid",
                            )
                        })?,
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
        created.value.status == RunStatus::Queued,
        ConformanceCase::CreateRunAtomicity,
        "fixture Run was not created atomically in queued state",
    )?;
    Ok(CreatedTaskRun {
        run_id,
        task_id,
        initial_checkpoint_id,
    })
}

fn claim_command(fixture: &Phase2aReliabilityFixture, tag: u16) -> ClaimTask {
    ClaimTask {
        worker_id: WorkerId::from_bytes(identity(fixture, tag)),
        lease_token: LeaseToken::from_bytes(identity_digest(fixture, tag)),
        lease_duration: DurationMicros::new(60_000_000),
        candidate_window: 8,
        kind: None,
    }
}

fn completion(
    fixture: &Phase2aReliabilityFixture,
    created: &CreatedTaskRun,
    claim: &ClaimedTask,
    lease: LeaseProof,
    completion_event_id: EventId,
    checkpoint_id: CheckpointId,
    next: NextActions,
) -> CompleteTask {
    CompleteTask {
        expected_run: expected_claim_run(created.run_id, claim),
        lease,
        completion_event_id,
        checkpoint: NewCheckpoint {
            checkpoint_id,
            sequence: 2,
            schema_version: 1,
            workflow_version_id: None,
            coordinator_agent_version_id: None,
            execution_generation: claim.task.generation,
            state: empty_payload(),
            state_digest: Digest::from_bytes(identity_digest(fixture, 901)),
            created_event_id: completion_event_id,
        },
        task_result: TaskResult {
            output: empty_payload(),
        },
        stage_mutation: None,
        additional_stage_mutations: Vec::new(),
        new_stages: Vec::new(),
        artifacts: Vec::new(),
        next,
    }
}

fn lease_proof(task_id: TaskId, command: &ClaimTask, generation: u64) -> LeaseProof {
    LeaseProof {
        task_id,
        worker_id: command.worker_id,
        token: command.lease_token.clone(),
        execution_generation: generation,
    }
}

async fn cancel_fixture(
    store: &dyn DurableStore,
    fixture: &Phase2aReliabilityFixture,
    run_id: RunId,
    tag: u16,
) -> Result<(), Phase2aReliabilityError> {
    let query = query_context(fixture);
    let run = store.get_run(&query, run_id).await?.ok_or_else(|| {
        invariant(
            ConformanceCase::UniqueTerminalEvent,
            "fixture Run disappeared",
        )
    })?;
    if !run.status.is_terminal() {
        let cancel_context = context(fixture, run_id, "phase2a_cleanup", tag)?;
        store
            .cancel_run(
                &cancel_context,
                ControlRun {
                    expected_run: ExpectedRun {
                        run_id,
                        version: Some(run.version),
                        execution_generation: Some(run.execution_generation),
                    },
                    event_id: event_id(fixture, tag + 1),
                    reason: "close Phase 2A conformance fixture".to_owned(),
                },
            )
            .await?;
    }
    Ok(())
}

fn expected_claim_run(run_id: RunId, claim: &ClaimedTask) -> ExpectedRun {
    ExpectedRun {
        run_id,
        version: Some(claim.run_version),
        execution_generation: Some(claim.task.generation),
    }
}

fn context(
    fixture: &Phase2aReliabilityFixture,
    run_id: RunId,
    scope: &str,
    tag: u16,
) -> Result<CommandContext, Phase2aReliabilityError> {
    Ok(CommandContext {
        tenant_id: fixture.tenant_id,
        command_id: CommandId::from_bytes(identity(fixture, tag)),
        correlation_id: CorrelationId::from_bytes(identity(fixture, tag)),
        actor_ref: fixture.actor_ref.clone(),
        scope: ScopeKey::parse(scope).map_err(|_| {
            invariant(
                ConformanceCase::CommandIdempotency,
                "conformance scope is invalid",
            )
        })?,
        idempotency_key: IdempotencyKey::parse(format!("{scope}-{run_id}-{tag}")).map_err(
            |_| {
                invariant(
                    ConformanceCase::CommandIdempotency,
                    "generated idempotency key is invalid",
                )
            },
        )?,
        request_hash: Digest::from_bytes(identity_digest(fixture, tag)),
    })
}

fn query_context(fixture: &Phase2aReliabilityFixture) -> QueryContext {
    QueryContext {
        tenant_id: fixture.tenant_id,
        actor_ref: fixture.actor_ref.clone(),
        authoritative: true,
    }
}

fn identity(fixture: &Phase2aReliabilityFixture, tag: u16) -> [u8; 16] {
    let mut value = fixture.identity_seed;
    let suffix = u16::from_be_bytes([value[14], value[15]]).wrapping_add(tag);
    let [high, low] = suffix.to_be_bytes();
    value[14] = high;
    value[15] = low;
    value
}

fn identity_digest(fixture: &Phase2aReliabilityFixture, tag: u16) -> [u8; 32] {
    let mut value = [0; 32];
    value[..16].copy_from_slice(&fixture.identity_seed);
    value[30..].copy_from_slice(&tag.to_be_bytes());
    value
}

fn task_id(fixture: &Phase2aReliabilityFixture, tag: u16) -> TaskId {
    TaskId::from_bytes(identity(fixture, tag))
}

fn event_id(fixture: &Phase2aReliabilityFixture, tag: u16) -> EventId {
    EventId::from_bytes(identity(fixture, tag))
}

fn checkpoint_id(fixture: &Phase2aReliabilityFixture, tag: u16) -> CheckpointId {
    CheckpointId::from_bytes(identity(fixture, tag))
}

fn empty_payload() -> JsonPayload {
    JsonPayload::from_validated_bytes(b"{}".to_vec())
}

fn require(
    condition: bool,
    case: ConformanceCase,
    detail: &'static str,
) -> Result<(), Phase2aReliabilityError> {
    if condition {
        Ok(())
    } else {
        Err(invariant(case, detail))
    }
}

const fn invariant(case: ConformanceCase, detail: &'static str) -> Phase2aReliabilityError {
    Phase2aReliabilityError::Invariant { case, detail }
}
