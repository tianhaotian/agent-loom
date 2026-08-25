use agent_loom_domain::{
    CheckpointId, EventId, JsonPayload, LogicalKey, RunId, RunSnapshot, RunStatus,
    StageExecutionId, StageStatus, TaskId, TaskKind, TaskSnapshot, TaskStatus, TenantId,
    UnixMicros, WorkflowVersionId,
};
use agent_loom_durable_store::{
    ClaimTask, ClaimedTask, CommandContext, CommandDisposition, Committed, CompleteTask,
    CompletionShapeError, CreateRun, DurableFollowUp, InitialTask, NewTask, NextActions,
    PostCommitHint, StoreError, StoreErrorCode, StoreResult,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio_postgres::{Client, Row, Transaction};
use uuid::Uuid;

const DEFAULT_RECEIPT_TTL_MICROS: u64 = 30 * 24 * 60 * 60 * 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PostgresTransactionConfig {
    pub receipt_ttl_micros: u64,
}

impl Default for PostgresTransactionConfig {
    fn default() -> Self {
        Self {
            receipt_ttl_micros: DEFAULT_RECEIPT_TTL_MICROS,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresTransactionExecutor {
    config: PostgresTransactionConfig,
}

impl PostgresTransactionExecutor {
    pub const fn new(config: PostgresTransactionConfig) -> Self {
        Self { config }
    }

    /// Creates the Run, first Event, first Checkpoint, initial Tasks, and command
    /// receipt in one database transaction.
    ///
    /// # Errors
    ///
    /// Returns a stable store error when the command shape is invalid, the
    /// idempotency key was reused, or PostgreSQL rejects the transaction.
    #[allow(clippy::too_many_lines)]
    pub async fn create_run(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: CreateRun,
    ) -> StoreResult<Committed<RunSnapshot>> {
        validate_create_run(&command)?;
        let transaction = client.transaction().await.map_err(map_database_error)?;
        let db_now = database_now(&transaction).await?;
        match acquire_receipt(
            &transaction,
            context,
            db_now,
            self.config.receipt_ttl_micros,
        )
        .await?
        {
            ReceiptGuard::Existing(receipt) => {
                let snapshot = decode_run_receipt(context.tenant_id, &receipt.outcome)?;
                transaction.commit().await.map_err(map_database_error)?;
                return Ok(committed_run(
                    CommandDisposition::Duplicate,
                    snapshot,
                    receipt.event_id.map(event_id),
                    Vec::new(),
                ));
            }
            ReceiptGuard::Acquired => {}
        }

        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_id = uuid(command.run_id.into_bytes());
        let workflow_version_id = command.workflow_version_id.map(|id| uuid(id.into_bytes()));
        let coordinator_agent_version_id = command
            .coordinator_agent_version_id
            .map(|id| uuid(id.into_bytes()));
        let checkpoint_id = uuid(command.initial_checkpoint.checkpoint_id.into_bytes());
        let initial_event_id = uuid(command.initial_event_id.into_bytes());
        let correlation_id = uuid(context.correlation_id.into_bytes());
        let input = json_value(&command.input)?;
        let checkpoint_state = json_value(&command.initial_checkpoint.state)?;
        let deadline = command.deadline.map(UnixMicros::get);
        let checkpoint_sequence =
            to_i64(command.initial_checkpoint.sequence, "checkpoint sequence")?;
        let checkpoint_schema_version = i64::from(command.initial_checkpoint.schema_version);
        let checkpoint_generation = to_i64(
            command.initial_checkpoint.execution_generation,
            "execution generation",
        )?;

        transaction
            .execute(
                "INSERT INTO agent_loom.runs (\
                    run_id, tenant_id, workflow_version_id, coordinator_agent_version_id, \
                    parent_run_id, parent_task_id, status, suspended_from_status, version, \
                    execution_generation, next_event_sequence, current_checkpoint_id, \
                    terminal_event_id, input_json, state_summary_json, deadline, \
                    resume_blocked_reason, created_by, created_at, updated_at, terminal_at\
                 ) VALUES ($1, $2, $3, $4, NULL, NULL, 'queued', NULL, 0, 0, 2, NULL, \
                    NULL, $5, '{}'::jsonb, to_timestamp(($6::bigint)::double precision / 1000000.0), \
                    NULL, $7, to_timestamp(($8::bigint)::double precision / 1000000.0), \
                    to_timestamp(($8::bigint)::double precision / 1000000.0), NULL)",
                &[
                    &run_id,
                    &tenant_id,
                    &workflow_version_id,
                    &coordinator_agent_version_id,
                    &input,
                    &deadline,
                    &context.actor_ref,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;

        insert_event(
            &transaction,
            EventInsert {
                event_id: initial_event_id,
                tenant_id,
                run_id,
                sequence: 1,
                event_type: "run.created",
                payload: &input,
                producer: "runtime",
                context,
                correlation_id,
                recorded_at: db_now,
            },
        )
        .await?;

        let checkpoint_workflow_version_id = command
            .initial_checkpoint
            .workflow_version_id
            .map(|id| uuid(id.into_bytes()));
        let checkpoint_agent_version_id = command
            .initial_checkpoint
            .coordinator_agent_version_id
            .map(|id| uuid(id.into_bytes()));
        let state_digest = command
            .initial_checkpoint
            .state_digest
            .as_bytes()
            .as_slice();
        transaction
            .execute(
                "INSERT INTO agent_loom.checkpoints (\
                    checkpoint_id, tenant_id, run_id, sequence, schema_version, \
                    workflow_version_id, coordinator_agent_version_id, execution_generation, \
                    state_json, state_digest, created_event_id, created_at\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, \
                    to_timestamp(($12::bigint)::double precision / 1000000.0))",
                &[
                    &checkpoint_id,
                    &tenant_id,
                    &run_id,
                    &checkpoint_sequence,
                    &checkpoint_schema_version,
                    &checkpoint_workflow_version_id,
                    &checkpoint_agent_version_id,
                    &checkpoint_generation,
                    &checkpoint_state,
                    &state_digest,
                    &initial_event_id,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;

        transaction
            .execute(
                "UPDATE agent_loom.runs SET current_checkpoint_id = $3 \
                 WHERE tenant_id = $1 AND run_id = $2",
                &[&tenant_id, &run_id, &checkpoint_id],
            )
            .await
            .map_err(map_database_error)?;

        for task in &command.initial_tasks {
            insert_initial_task(
                &transaction,
                tenant_id,
                run_id,
                initial_event_id,
                checkpoint_sequence,
                db_now,
                task,
            )
            .await?;
        }

        let snapshot = RunSnapshot {
            tenant_id: context.tenant_id,
            run_id: command.run_id,
            workflow_version_id: command.workflow_version_id,
            status: RunStatus::Queued,
            suspended_from_status: None,
            version: 0,
            execution_generation: 0,
            next_event_sequence: 2,
            current_checkpoint_id: Some(command.initial_checkpoint.checkpoint_id),
            terminal_event_id: None,
            deadline: command.deadline,
            updated_at: UnixMicros::new(db_now),
        };
        let outcome = encode_run_receipt(&snapshot)?;
        finish_receipt(
            &transaction,
            context,
            "applied",
            &outcome,
            Some(initial_event_id),
            Some(("run", run_id, 0)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;

        let follow_ups = command
            .initial_tasks
            .iter()
            .map(|task| DurableFollowUp::Task {
                task_id: task.task_id,
            })
            .collect();
        Ok(committed_run(
            CommandDisposition::Applied,
            snapshot,
            Some(command.initial_event_id),
            follow_ups,
        ))
    }

    /// Claims one due task using `FOR UPDATE SKIP LOCKED`, writes the lease and
    /// `TaskAttempt`, advances the Run event sequence, and records the idempotent outcome.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for idempotency misuse or database failures.
    #[allow(clippy::too_many_lines)]
    pub async fn claim_task(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: ClaimTask,
    ) -> StoreResult<Option<Committed<ClaimedTask>>> {
        if command.lease_duration.get() == 0 || command.candidate_window == 0 {
            return Err(invalid_command(
                "claim lease and candidate window must be positive",
            ));
        }
        let transaction = client.transaction().await.map_err(map_database_error)?;
        let db_now = database_now(&transaction).await?;
        match acquire_receipt(
            &transaction,
            context,
            db_now,
            self.config.receipt_ttl_micros,
        )
        .await?
        {
            ReceiptGuard::Existing(receipt) => {
                let result = decode_claim_receipt(context.tenant_id, &receipt.outcome)?;
                transaction.commit().await.map_err(map_database_error)?;
                return Ok(result.map(|value| Committed {
                    disposition: CommandDisposition::Duplicate,
                    value,
                    event_ids: receipt.event_id.map(event_id).into_iter().collect(),
                    durable_follow_ups: Vec::new(),
                    post_commit_hints: Vec::new(),
                }));
            }
            ReceiptGuard::Acquired => {}
        }

        let tenant_id = uuid(context.tenant_id.into_bytes());
        let candidate = transaction
            .query_opt(
                "SELECT t.task_id, t.run_id, t.stage_execution_id, t.logical_key, t.kind, \
                        t.generation, t.attempt, t.max_attempts, \
                        (extract(epoch FROM t.available_at) * 1000000)::bigint, \
                        r.next_event_sequence \
                 FROM agent_loom.tasks t \
                 JOIN agent_loom.runs r \
                   ON r.tenant_id = t.tenant_id AND r.run_id = t.run_id \
                 WHERE t.tenant_id = $1 \
                   AND t.status IN ('queued', 'retry_scheduled') \
                   AND t.available_at <= \
                       to_timestamp(($2::bigint)::double precision / 1000000.0) \
                   AND (t.deadline IS NULL OR t.deadline >= \
                       to_timestamp(($2::bigint)::double precision / 1000000.0)) \
                   AND t.attempt < t.max_attempts \
                   AND r.status IN ('queued', 'running') \
                   AND t.generation = r.execution_generation \
                 ORDER BY t.priority DESC, t.available_at, t.task_id \
                 LIMIT 1 FOR UPDATE OF t, r SKIP LOCKED",
                &[&tenant_id, &db_now],
            )
            .await
            .map_err(map_database_error)?;

        let Some(row) = candidate else {
            let outcome = json!({"type": "claim_none"});
            finish_receipt(&transaction, context, "no_op", &outcome, None, None).await?;
            transaction.commit().await.map_err(map_database_error)?;
            return Ok(None);
        };

        let task_id: Uuid = row.get(0);
        let run_id: Uuid = row.get(1);
        let stage_execution_id: Option<Uuid> = row.get(2);
        let logical_key: String = row.get(3);
        let kind: String = row.get(4);
        let generation = nonnegative_u64(row.get(5), "task generation")?;
        let previous_attempt: i64 = row.get(6);
        let max_attempts = positive_u32(row.get(7), "task max attempts")?;
        let available_at: i64 = row.get(8);
        let event_sequence: i64 = row.get(9);
        let attempt = previous_attempt
            .checked_add(1)
            .ok_or_else(|| invalid_command("task attempt overflow"))?;
        let lease_duration = to_i64(command.lease_duration.get(), "lease duration")?;
        let lease_expires_at = db_now
            .checked_add(lease_duration)
            .ok_or_else(|| invalid_command("lease expiry overflow"))?;
        let worker_id = uuid(command.worker_id.into_bytes());
        let lease_token = command.lease_token.as_bytes().as_slice();

        transaction
            .execute(
                "UPDATE agent_loom.tasks SET status = 'leased', attempt = $3, \
                    lease_owner = $4, lease_token = $5, \
                    lease_expires_at = to_timestamp(($6::bigint)::double precision / 1000000.0), \
                    updated_at = to_timestamp(($7::bigint)::double precision / 1000000.0) \
                 WHERE tenant_id = $1 AND task_id = $2",
                &[
                    &tenant_id,
                    &task_id,
                    &attempt,
                    &worker_id,
                    &lease_token,
                    &lease_expires_at,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;

        let attempt_id = deterministic_attempt_id(task_id, attempt, lease_token);
        let lease_digest: [u8; 32] = Sha256::digest(lease_token).into();
        transaction
            .execute(
                "INSERT INTO agent_loom.task_attempts (\
                    task_attempt_id, tenant_id, task_id, run_id, attempt, worker_id, \
                    lease_token_digest, claimed_at, lease_expires_at, finished_at, \
                    outcome, error_code, metrics_json\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, \
                    to_timestamp(($8::bigint)::double precision / 1000000.0), \
                    to_timestamp(($9::bigint)::double precision / 1000000.0), \
                    NULL, NULL, NULL, '{}'::jsonb)",
                &[
                    &attempt_id,
                    &tenant_id,
                    &task_id,
                    &run_id,
                    &attempt,
                    &worker_id,
                    &lease_digest.as_slice(),
                    &db_now,
                    &lease_expires_at,
                ],
            )
            .await
            .map_err(map_database_error)?;

        transaction
            .execute(
                "UPDATE agent_loom.runs SET status = 'running', version = version + 1, \
                    next_event_sequence = next_event_sequence + 1, \
                    updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
                 WHERE tenant_id = $1 AND run_id = $2",
                &[&tenant_id, &run_id, &db_now],
            )
            .await
            .map_err(map_database_error)?;
        let event_id = uuid(context.command_id.into_bytes());
        let correlation_id = uuid(context.correlation_id.into_bytes());
        let event_payload = json!({
            "task_id": task_id,
            "worker_id": worker_id,
            "attempt": attempt,
            "lease_expires_at_micros": lease_expires_at,
        });
        insert_event(
            &transaction,
            EventInsert {
                event_id,
                tenant_id,
                run_id,
                sequence: event_sequence,
                event_type: "task.claimed",
                payload: &event_payload,
                producer: "worker",
                context,
                correlation_id,
                recorded_at: db_now,
            },
        )
        .await?;

        let value = ClaimedTask {
            task: TaskSnapshot {
                tenant_id: context.tenant_id,
                task_id: task_id_from_uuid(task_id),
                run_id: run_id_from_uuid(run_id),
                stage_execution_id: stage_execution_id.map(stage_id_from_uuid),
                logical_key: LogicalKey::parse(logical_key)
                    .map_err(|_| inconsistent("database contains an invalid task logical key"))?,
                kind: parse_task_kind(&kind)?,
                status: TaskStatus::Leased,
                generation,
                attempt: positive_u32(attempt, "task attempt")?,
                max_attempts,
                available_at: UnixMicros::new(available_at),
            },
            lease_expires_at: UnixMicros::new(lease_expires_at),
        };
        let outcome = encode_claim_receipt(Some(&value))?;
        finish_receipt(
            &transaction,
            context,
            "applied",
            &outcome,
            Some(event_id),
            Some(("task", task_id, attempt)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;
        Ok(Some(Committed {
            disposition: CommandDisposition::Applied,
            value,
            event_ids: vec![event_id_from_uuid(event_id)],
            durable_follow_ups: Vec::new(),
            post_commit_hints: vec![
                PostCommitHint::RunEventsAvailable {
                    run_id: run_id_from_uuid(run_id),
                },
                PostCommitHint::InvalidateRunCache {
                    run_id: run_id_from_uuid(run_id),
                },
            ],
        }))
    }

    /// Atomically validates the lease and Run fence, appends the completion
    /// Event and Checkpoint, finalizes the Task, applies stage/artifact writes,
    /// schedules the next action, and advances the Run projection.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for invalid command shape, a lost/expired
    /// lease, a Run CAS conflict, or a failed database transaction.
    #[allow(clippy::too_many_lines)]
    pub async fn complete_task(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: CompleteTask,
    ) -> StoreResult<Committed<RunSnapshot>> {
        command.validate_shape().map_err(map_completion_shape)?;
        let transaction = client.transaction().await.map_err(map_database_error)?;
        let db_now = database_now(&transaction).await?;
        match acquire_receipt(
            &transaction,
            context,
            db_now,
            self.config.receipt_ttl_micros,
        )
        .await?
        {
            ReceiptGuard::Existing(receipt) => {
                let snapshot = decode_run_receipt(context.tenant_id, &receipt.outcome)?;
                transaction.commit().await.map_err(map_database_error)?;
                return Ok(committed_run(
                    CommandDisposition::Duplicate,
                    snapshot,
                    receipt.event_id.map(event_id),
                    Vec::new(),
                ));
            }
            ReceiptGuard::Acquired => {}
        }

        let tenant_id = uuid(context.tenant_id.into_bytes());
        let task_id = uuid(command.lease.task_id.into_bytes());
        let row = transaction
            .query_opt(
                "SELECT t.run_id, t.status, t.generation, t.attempt, t.lease_owner, \
                        t.lease_token, \
                        (extract(epoch FROM t.lease_expires_at) * 1000000)::bigint, \
                        r.status, r.version, r.execution_generation, r.next_event_sequence, \
                        c.sequence, r.workflow_version_id, r.coordinator_agent_version_id, \
                        CASE WHEN r.deadline IS NULL THEN NULL \
                             ELSE (extract(epoch FROM r.deadline) * 1000000)::bigint END \
                 FROM agent_loom.tasks t \
                 JOIN agent_loom.runs r ON r.tenant_id = t.tenant_id AND r.run_id = t.run_id \
                 LEFT JOIN agent_loom.checkpoints c ON c.tenant_id = r.tenant_id \
                    AND c.run_id = r.run_id AND c.checkpoint_id = r.current_checkpoint_id \
                 WHERE t.tenant_id = $1 AND t.task_id = $2 \
                 FOR UPDATE OF t, r",
                &[&tenant_id, &task_id],
            )
            .await
            .map_err(map_database_error)?
            .ok_or_else(|| store_error(StoreErrorCode::NotFound, "task was not found"))?;
        let locked = LockedCompletion::decode(&row);
        validate_completion_fences(&command, &locked, db_now)?;

        let run_id = locked.run_id;
        let event_id = uuid(command.completion_event_id.into_bytes());
        let correlation_id = uuid(context.correlation_id.into_bytes());
        let event_payload = json_value(&command.task_result.output)?;
        insert_event(
            &transaction,
            EventInsert {
                event_id,
                tenant_id,
                run_id,
                sequence: locked.next_event_sequence,
                event_type: "task.completed",
                payload: &event_payload,
                producer: "worker",
                context,
                correlation_id,
                recorded_at: db_now,
            },
        )
        .await?;

        insert_completion_checkpoint(&transaction, tenant_id, run_id, event_id, db_now, &command)
            .await?;
        finalize_task(
            &transaction,
            tenant_id,
            task_id,
            locked.attempt,
            db_now,
            &command,
        )
        .await?;
        apply_stage_mutation(
            &transaction,
            tenant_id,
            run_id,
            db_now,
            command.stage_mutation,
        )
        .await?;
        insert_artifacts(&transaction, tenant_id, run_id, task_id, db_now, &command).await?;

        let transition =
            apply_next_actions(&transaction, tenant_id, run_id, event_id, db_now, &command).await?;
        let checkpoint_id = uuid(command.checkpoint.checkpoint_id.into_bytes());
        let next_event_sequence = locked
            .next_event_sequence
            .checked_add(1)
            .ok_or_else(|| inconsistent("run event sequence overflow"))?;
        let state_summary = match &command.next {
            NextActions::FinishRun(result) => json_value(&result.output)?,
            NextActions::Tasks(_)
            | NextActions::Wait(_)
            | NextActions::Retry(_)
            | NextActions::NoFurtherWork => json_value(&command.checkpoint.state)?,
        };
        let updated = transaction
            .execute(
                "UPDATE agent_loom.runs SET status = $5, suspended_from_status = NULL, \
                    version = version + 1, next_event_sequence = $6, \
                    current_checkpoint_id = $7, state_summary_json = $8, \
                    terminal_event_id = CASE WHEN $9 THEN $10 ELSE NULL END, \
                    terminal_at = CASE WHEN $9 THEN \
                        to_timestamp(($11::bigint)::double precision / 1000000.0) ELSE NULL END, \
                    updated_at = to_timestamp(($11::bigint)::double precision / 1000000.0) \
                 WHERE tenant_id = $1 AND run_id = $2 AND version = $3 \
                   AND execution_generation = $4 \
                   AND status IN ('queued', 'running', 'waiting', 'approval_required', 'retrying')",
                &[
                    &tenant_id,
                    &run_id,
                    &locked.run_version,
                    &locked.execution_generation,
                    &transition.status,
                    &next_event_sequence,
                    &checkpoint_id,
                    &state_summary,
                    &transition.terminal,
                    &event_id,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;
        if updated != 1 {
            return Err(store_error(
                StoreErrorCode::VersionConflict,
                "run changed while completing the task",
            ));
        }

        let snapshot = RunSnapshot {
            tenant_id: context.tenant_id,
            run_id: run_id_from_uuid(run_id),
            workflow_version_id: locked.workflow_version_id.map(workflow_id_from_uuid),
            status: parse_run_status(transition.status)?,
            suspended_from_status: None,
            version: nonnegative_u64(locked.run_version + 1, "run version")?,
            execution_generation: nonnegative_u64(
                locked.execution_generation,
                "execution generation",
            )?,
            next_event_sequence: nonnegative_u64(next_event_sequence, "event sequence")?,
            current_checkpoint_id: Some(command.checkpoint.checkpoint_id),
            terminal_event_id: transition.terminal.then_some(command.completion_event_id),
            deadline: locked.deadline.map(UnixMicros::new),
            updated_at: UnixMicros::new(db_now),
        };
        let outcome = encode_run_receipt(&snapshot)?;
        finish_receipt(
            &transaction,
            context,
            "applied",
            &outcome,
            Some(event_id),
            Some(("run", run_id, locked.run_version + 1)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;
        Ok(committed_run(
            CommandDisposition::Applied,
            snapshot,
            Some(command.completion_event_id),
            transition.follow_ups,
        ))
    }
}

#[derive(Debug)]
enum ReceiptGuard {
    Acquired,
    Existing(ExistingReceipt),
}

#[derive(Debug)]
struct ExistingReceipt {
    outcome: Value,
    event_id: Option<Uuid>,
}

async fn acquire_receipt(
    transaction: &Transaction<'_>,
    context: &CommandContext,
    db_now: i64,
    ttl_micros: u64,
) -> StoreResult<ReceiptGuard> {
    let ttl = to_i64(ttl_micros, "receipt TTL")?;
    let receipt_id = uuid(context.command_id.into_bytes());
    let tenant_id = uuid(context.tenant_id.into_bytes());
    let request_hash = context.request_hash.as_bytes().as_slice();
    let inserted = transaction
        .execute(
            "INSERT INTO agent_loom.command_receipts (\
                receipt_id, tenant_id, scope, idempotency_key, request_hash, outcome_kind, \
                outcome_json, event_id, resource_type, resource_id, resource_version, \
                created_at, expires_at\
             ) VALUES ($1, $2, $3, $4, $5, 'outcome_unknown', '{}'::jsonb, NULL, NULL, \
                NULL, NULL, to_timestamp(($6::bigint)::double precision / 1000000.0), \
                to_timestamp((($6::bigint + $7::bigint))::double precision / 1000000.0)) \
             ON CONFLICT (tenant_id, scope, idempotency_key) DO NOTHING",
            &[
                &receipt_id,
                &tenant_id,
                &context.scope.as_str(),
                &context.idempotency_key.as_str(),
                &request_hash,
                &db_now,
                &ttl,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if inserted == 1 {
        return Ok(ReceiptGuard::Acquired);
    }

    let row = transaction
        .query_one(
            "SELECT request_hash, outcome_kind, outcome_json, event_id \
             FROM agent_loom.command_receipts \
             WHERE tenant_id = $1 AND scope = $2 AND idempotency_key = $3 FOR UPDATE",
            &[
                &tenant_id,
                &context.scope.as_str(),
                &context.idempotency_key.as_str(),
            ],
        )
        .await
        .map_err(map_database_error)?;
    let stored_hash: Vec<u8> = row.get(0);
    if stored_hash.as_slice() != request_hash {
        return Err(store_error(
            StoreErrorCode::IdempotencyKeyReused,
            "idempotency key was reused with a different request",
        ));
    }
    let outcome_kind: &str = row.get(1);
    if outcome_kind == "outcome_unknown" {
        return Err(StoreError::new(
            StoreErrorCode::OutcomeUnknown,
            agent_loom_durable_store::RetryClass::Reconcile,
            "the first command outcome requires reconciliation",
        ));
    }
    Ok(ReceiptGuard::Existing(ExistingReceipt {
        outcome: row.get(2),
        event_id: row.get(3),
    }))
}

async fn finish_receipt(
    transaction: &Transaction<'_>,
    context: &CommandContext,
    outcome_kind: &str,
    outcome: &Value,
    event_id: Option<Uuid>,
    resource: Option<(&str, Uuid, i64)>,
) -> StoreResult<()> {
    let tenant_id = uuid(context.tenant_id.into_bytes());
    let (resource_type, resource_id, resource_version) = resource
        .map_or((None, None, None), |(kind, id, version)| {
            (Some(kind), Some(id), Some(version))
        });
    let updated = transaction
        .execute(
            "UPDATE agent_loom.command_receipts SET outcome_kind = $4, outcome_json = $5, \
                event_id = $6, resource_type = $7, resource_id = $8, resource_version = $9 \
             WHERE tenant_id = $1 AND scope = $2 AND idempotency_key = $3 \
               AND outcome_kind = 'outcome_unknown'",
            &[
                &tenant_id,
                &context.scope.as_str(),
                &context.idempotency_key.as_str(),
                &outcome_kind,
                &outcome,
                &event_id,
                &resource_type,
                &resource_id,
                &resource_version,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if updated != 1 {
        return Err(inconsistent("command receipt guard was lost"));
    }
    Ok(())
}

async fn database_now(transaction: &Transaction<'_>) -> StoreResult<i64> {
    transaction
        .query_one(
            "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint",
            &[],
        )
        .await
        .map(|row| row.get(0))
        .map_err(map_database_error)
}

struct EventInsert<'a> {
    event_id: Uuid,
    tenant_id: Uuid,
    run_id: Uuid,
    sequence: i64,
    event_type: &'a str,
    payload: &'a Value,
    producer: &'a str,
    context: &'a CommandContext,
    correlation_id: Uuid,
    recorded_at: i64,
}

async fn insert_event(transaction: &Transaction<'_>, event: EventInsert<'_>) -> StoreResult<()> {
    transaction
        .execute(
            "INSERT INTO agent_loom.events (\
                event_id, tenant_id, run_id, sequence, event_type, payload_json, \
                payload_schema_version, producer, actor_ref, correlation_id, causation_id, \
                idempotency_key, occurred_at, recorded_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, $9, NULL, $10, NULL, \
                to_timestamp(($11::bigint)::double precision / 1000000.0))",
            &[
                &event.event_id,
                &event.tenant_id,
                &event.run_id,
                &event.sequence,
                &event.event_type,
                &event.payload,
                &event.producer,
                &event.context.actor_ref,
                &event.correlation_id,
                &event.context.idempotency_key.as_str(),
                &event.recorded_at,
            ],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

async fn insert_initial_task(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    event_id: Uuid,
    checkpoint_sequence: i64,
    db_now: i64,
    task: &InitialTask,
) -> StoreResult<()> {
    if task.max_attempts == 0 {
        return Err(invalid_command(
            "initial task max attempts must be positive",
        ));
    }
    let task_id = uuid(task.task_id.into_bytes());
    let stage_id = task.stage_execution_id.map(|id| uuid(id.into_bytes()));
    let kind = task_kind(task.kind);
    let available_at = task.available_at.get();
    let max_attempts = i64::from(task.max_attempts);
    let input = json_value(&task.input)?;
    transaction
        .execute(
            "INSERT INTO agent_loom.tasks (\
                task_id, tenant_id, run_id, stage_execution_id, logical_key, kind, status, \
                generation, based_on_checkpoint_sequence, priority, available_at, attempt, \
                max_attempts, lease_owner, lease_token, lease_expires_at, input_json, \
                result_json, error_code, error_json, deadline, created_event_id, created_at, \
                updated_at, completed_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, 'queued', 0, $7, $8, \
                to_timestamp(($9::bigint)::double precision / 1000000.0), 0, $10, NULL, NULL, NULL, \
                $11, NULL, NULL, NULL, NULL, $12, \
                to_timestamp(($13::bigint)::double precision / 1000000.0), \
                to_timestamp(($13::bigint)::double precision / 1000000.0), NULL)",
            &[
                &task_id,
                &tenant_id,
                &run_id,
                &stage_id,
                &task.logical_key.as_str(),
                &kind,
                &checkpoint_sequence,
                &task.priority,
                &available_at,
                &max_attempts,
                &input,
                &event_id,
                &db_now,
            ],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

#[derive(Debug)]
struct LockedCompletion {
    run_id: Uuid,
    task_status: String,
    task_generation: i64,
    attempt: i64,
    lease_owner: Option<Uuid>,
    lease_token: Option<Vec<u8>>,
    lease_expires_at: Option<i64>,
    run_status: String,
    run_version: i64,
    execution_generation: i64,
    next_event_sequence: i64,
    checkpoint_sequence: Option<i64>,
    workflow_version_id: Option<Uuid>,
    coordinator_agent_version_id: Option<Uuid>,
    deadline: Option<i64>,
}

impl LockedCompletion {
    fn decode(row: &Row) -> Self {
        Self {
            run_id: row.get(0),
            task_status: row.get(1),
            task_generation: row.get(2),
            attempt: row.get(3),
            lease_owner: row.get(4),
            lease_token: row.get(5),
            lease_expires_at: row.get(6),
            run_status: row.get(7),
            run_version: row.get(8),
            execution_generation: row.get(9),
            next_event_sequence: row.get(10),
            checkpoint_sequence: row.get(11),
            workflow_version_id: row.get(12),
            coordinator_agent_version_id: row.get(13),
            deadline: row.get(14),
        }
    }
}

fn validate_completion_fences(
    command: &CompleteTask,
    locked: &LockedCompletion,
    db_now: i64,
) -> StoreResult<()> {
    if run_id_from_uuid(locked.run_id) != command.expected_run.run_id {
        return Err(store_error(
            StoreErrorCode::NotFound,
            "task belongs to another run",
        ));
    }
    if locked.task_status != "leased" {
        return Err(store_error(StoreErrorCode::LeaseLost, "task is not leased"));
    }
    if locked.run_status == "paused" {
        return Err(store_error(
            StoreErrorCode::InvalidTransition,
            "paused run rejects task completion",
        ));
    }
    if matches!(
        locked.run_status.as_str(),
        "completed" | "failed" | "cancelled" | "timed_out"
    ) {
        return Err(store_error(StoreErrorCode::TerminalRun, "run is terminal"));
    }
    if command
        .expected_run
        .version
        .is_some_and(|expected| i64::try_from(expected).ok() != Some(locked.run_version))
    {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "run version changed",
        ));
    }
    let expected_generation = to_i64(command.lease.execution_generation, "lease generation")?;
    if locked.execution_generation != expected_generation
        || locked.task_generation != expected_generation
    {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "execution generation changed",
        ));
    }
    let expected_owner = uuid(command.lease.worker_id.into_bytes());
    if locked.lease_owner != Some(expected_owner)
        || locked.lease_token.as_deref() != Some(command.lease.token.as_bytes().as_slice())
    {
        return Err(store_error(
            StoreErrorCode::LeaseLost,
            "task lease proof does not match",
        ));
    }
    if locked
        .lease_expires_at
        .is_none_or(|expires| expires <= db_now)
    {
        return Err(store_error(
            StoreErrorCode::LeaseExpired,
            "task lease expired",
        ));
    }
    let checkpoint_workflow = command
        .checkpoint
        .workflow_version_id
        .map(|id| uuid(id.into_bytes()));
    let checkpoint_agent = command
        .checkpoint
        .coordinator_agent_version_id
        .map(|id| uuid(id.into_bytes()));
    if checkpoint_workflow != locked.workflow_version_id
        || checkpoint_agent != locked.coordinator_agent_version_id
    {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "checkpoint definition versions do not match the run",
        ));
    }
    let expected_checkpoint = locked.checkpoint_sequence.unwrap_or(0) + 1;
    if i64::try_from(command.checkpoint.sequence).ok() != Some(expected_checkpoint) {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "checkpoint sequence is not the next sequence",
        ));
    }
    Ok(())
}

async fn insert_completion_checkpoint(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    event_id: Uuid,
    db_now: i64,
    command: &CompleteTask,
) -> StoreResult<()> {
    let checkpoint = &command.checkpoint;
    let checkpoint_id = uuid(checkpoint.checkpoint_id.into_bytes());
    let sequence = to_i64(checkpoint.sequence, "checkpoint sequence")?;
    let schema_version = i64::from(checkpoint.schema_version);
    let workflow_version_id = checkpoint
        .workflow_version_id
        .map(|id| uuid(id.into_bytes()));
    let agent_version_id = checkpoint
        .coordinator_agent_version_id
        .map(|id| uuid(id.into_bytes()));
    let generation = to_i64(checkpoint.execution_generation, "checkpoint generation")?;
    let state = json_value(&checkpoint.state)?;
    let digest = checkpoint.state_digest.as_bytes().as_slice();
    let inserted = transaction
        .execute(
            "INSERT INTO agent_loom.checkpoints (\
                checkpoint_id, tenant_id, run_id, sequence, schema_version, \
                workflow_version_id, coordinator_agent_version_id, execution_generation, \
                state_json, state_digest, created_event_id, created_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, \
                to_timestamp(($12::bigint)::double precision / 1000000.0))",
            &[
                &checkpoint_id,
                &tenant_id,
                &run_id,
                &sequence,
                &schema_version,
                &workflow_version_id,
                &agent_version_id,
                &generation,
                &state,
                &digest,
                &event_id,
                &db_now,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if inserted != 1 {
        return Err(inconsistent("checkpoint insert did not affect one row"));
    }
    Ok(())
}

async fn finalize_task(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    task_id: Uuid,
    attempt: i64,
    db_now: i64,
    command: &CompleteTask,
) -> StoreResult<()> {
    let result = json_value(&command.task_result.output)?;
    let updated = transaction
        .execute(
            "UPDATE agent_loom.tasks SET status = 'succeeded', result_json = $3, \
                lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, \
                updated_at = to_timestamp(($4::bigint)::double precision / 1000000.0), \
                completed_at = to_timestamp(($4::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND task_id = $2 AND status = 'leased'",
            &[&tenant_id, &task_id, &result, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    if updated != 1 {
        return Err(store_error(
            StoreErrorCode::LeaseLost,
            "task lease was lost",
        ));
    }
    let attempts_updated = transaction
        .execute(
            "UPDATE agent_loom.task_attempts SET \
                finished_at = to_timestamp(($4::bigint)::double precision / 1000000.0), \
                outcome = 'succeeded' \
             WHERE tenant_id = $1 AND task_id = $2 AND attempt = $3 AND finished_at IS NULL",
            &[&tenant_id, &task_id, &attempt, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    if attempts_updated != 1 {
        return Err(inconsistent("leased task has no open matching attempt"));
    }
    Ok(())
}

async fn apply_stage_mutation(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    db_now: i64,
    mutation: Option<agent_loom_durable_store::StageMutation>,
) -> StoreResult<()> {
    let Some(mutation) = mutation else {
        return Ok(());
    };
    let stage_id = uuid(mutation.stage_execution_id.into_bytes());
    let expected_version = to_i64(mutation.expected_version, "stage version")?;
    let status = stage_status(mutation.target_status);
    let terminal = mutation.target_status.is_terminal();
    let updated = transaction
        .execute(
            "UPDATE agent_loom.stage_executions SET status = $5, version = version + 1, \
                completed_at = CASE WHEN $6 THEN \
                    to_timestamp(($4::bigint)::double precision / 1000000.0) ELSE NULL END, \
                updated_at = to_timestamp(($4::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND stage_execution_id = $3 \
               AND version = $7 \
               AND (\
                    (status = 'planned' AND $5 IN ('active', 'cancelled')) \
                 OR (status = 'active' AND $5 IN (\
                        'waiting_approval', 'rework_required', 'succeeded', 'failed', \
                        'skipped', 'cancelled')) \
                 OR (status = 'waiting_approval' AND $5 IN (\
                        'active', 'succeeded', 'rework_required', 'failed', 'cancelled')) \
                 OR (status = 'rework_required' AND $5 IN ('active', 'failed', 'cancelled'))\
               )",
            &[
                &tenant_id,
                &run_id,
                &stage_id,
                &db_now,
                &status,
                &terminal,
                &expected_version,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if updated != 1 {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "stage version changed",
        ));
    }
    Ok(())
}

async fn insert_artifacts(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    task_id: Uuid,
    db_now: i64,
    command: &CompleteTask,
) -> StoreResult<()> {
    for artifact in &command.artifacts {
        let artifact_id = uuid(artifact.artifact_id.into_bytes());
        let stage_id = artifact.stage_execution_id.map(|id| uuid(id.into_bytes()));
        let contract_version = i64::from(artifact.contract_version);
        let version = to_i64(artifact.version, "artifact version")?;
        let size_bytes = to_i64(artifact.size_bytes, "artifact size")?;
        let digest = artifact.digest.as_bytes().as_slice();
        let sources: Vec<_> = artifact
            .sources
            .iter()
            .map(|source| {
                json!({
                    "artifact_id": uuid(source.artifact_id.into_bytes()),
                    "version": source.version,
                })
            })
            .collect();
        let sources = Value::Array(sources);
        let metadata = json_value(&artifact.metadata)?;
        let event_id = uuid(artifact.created_event_id.into_bytes());
        transaction
            .execute(
                "INSERT INTO agent_loom.artifact_refs (\
                    artifact_id, tenant_id, run_id, stage_execution_id, task_id, logical_key, \
                    kind, contract_version, version, uri, digest, media_type, size_bytes, \
                    source_artifact_refs_json, metadata_json, produced_by, created_event_id, created_at\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
                    $14, $15, $16, $17, \
                    to_timestamp(($18::bigint)::double precision / 1000000.0))",
                &[
                    &artifact_id,
                    &tenant_id,
                    &run_id,
                    &stage_id,
                    &task_id,
                    &artifact.logical_key.as_str(),
                    &artifact.kind,
                    &contract_version,
                    &version,
                    &artifact.uri,
                    &digest,
                    &artifact.media_type,
                    &size_bytes,
                    &sources,
                    &metadata,
                    &artifact.produced_by,
                    &event_id,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;
    }
    Ok(())
}

#[derive(Debug)]
struct NextTransition {
    status: &'static str,
    terminal: bool,
    follow_ups: Vec<DurableFollowUp>,
}

async fn apply_next_actions(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    event_id: Uuid,
    db_now: i64,
    command: &CompleteTask,
) -> StoreResult<NextTransition> {
    match &command.next {
        NextActions::Tasks(tasks) => {
            for task in tasks {
                insert_new_task(
                    transaction,
                    tenant_id,
                    run_id,
                    event_id,
                    db_now,
                    task,
                    "queued",
                )
                .await?;
            }
            Ok(NextTransition {
                status: "queued",
                terminal: false,
                follow_ups: tasks
                    .iter()
                    .map(|task| DurableFollowUp::Task {
                        task_id: task.task_id,
                    })
                    .collect(),
            })
        }
        NextActions::Retry(retry) => {
            insert_new_task(
                transaction,
                tenant_id,
                run_id,
                event_id,
                db_now,
                &retry.task,
                "retry_scheduled",
            )
            .await?;
            Ok(NextTransition {
                status: "retrying",
                terminal: false,
                follow_ups: vec![DurableFollowUp::Task {
                    task_id: retry.task.task_id,
                }],
            })
        }
        NextActions::Wait(wait) => {
            let wait_id = uuid(wait.wait_id.into_bytes());
            let stage_id = wait.stage_execution_id.map(|id| uuid(id.into_bytes()));
            let match_hash = wait.match_key_hash.as_bytes().as_slice();
            let contract = json_value(&wait.match_contract)?;
            let expires_at = wait.expires_at.map(UnixMicros::get);
            transaction
                .execute(
                    "INSERT INTO agent_loom.wait_subscriptions (\
                        wait_id, tenant_id, run_id, stage_execution_id, wait_type, \
                        expected_event_type, match_key_hash, match_contract_json, status, \
                        active_slot, expires_at, consumed_by_event_id, created_event_id, \
                        created_at, consumed_at, updated_at\
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open', 1, \
                        to_timestamp(($9::bigint)::double precision / 1000000.0), NULL, $10, \
                        to_timestamp(($11::bigint)::double precision / 1000000.0), NULL, \
                        to_timestamp(($11::bigint)::double precision / 1000000.0))",
                    &[
                        &wait_id,
                        &tenant_id,
                        &run_id,
                        &stage_id,
                        &wait.wait_type,
                        &wait.expected_event_type,
                        &match_hash,
                        &contract,
                        &expires_at,
                        &event_id,
                        &db_now,
                    ],
                )
                .await
                .map_err(map_database_error)?;
            Ok(NextTransition {
                status: if wait.wait_type == "approval" {
                    "approval_required"
                } else {
                    "waiting"
                },
                terminal: false,
                follow_ups: Vec::new(),
            })
        }
        NextActions::FinishRun(result) => Ok(NextTransition {
            status: run_status(result.status),
            terminal: true,
            follow_ups: Vec::new(),
        }),
        NextActions::NoFurtherWork => Ok(NextTransition {
            status: "running",
            terminal: false,
            follow_ups: Vec::new(),
        }),
    }
}

async fn insert_new_task(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    event_id: Uuid,
    db_now: i64,
    task: &NewTask,
    status: &str,
) -> StoreResult<()> {
    let task_id = uuid(task.task_id.into_bytes());
    let stage_id = task.stage_execution_id.map(|id| uuid(id.into_bytes()));
    let generation = to_i64(task.generation, "task generation")?;
    let checkpoint_sequence = to_i64(task.based_on_checkpoint_sequence, "checkpoint sequence")?;
    let max_attempts = i64::from(task.max_attempts);
    let available_at = task.available_at.get();
    let deadline = task.deadline.map(UnixMicros::get);
    let input = json_value(&task.input)?;
    let kind = task_kind(task.kind);
    transaction
        .execute(
            "INSERT INTO agent_loom.tasks (\
                task_id, tenant_id, run_id, stage_execution_id, logical_key, kind, status, \
                generation, based_on_checkpoint_sequence, priority, available_at, attempt, \
                max_attempts, lease_owner, lease_token, lease_expires_at, input_json, \
                result_json, error_code, error_json, deadline, created_event_id, created_at, \
                updated_at, completed_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
                to_timestamp(($11::bigint)::double precision / 1000000.0), 0, $12, NULL, NULL, NULL, \
                $13, NULL, NULL, NULL, to_timestamp(($14::bigint)::double precision / 1000000.0), \
                $15, to_timestamp(($16::bigint)::double precision / 1000000.0), \
                to_timestamp(($16::bigint)::double precision / 1000000.0), NULL)",
            &[
                &task_id,
                &tenant_id,
                &run_id,
                &stage_id,
                &task.logical_key.as_str(),
                &kind,
                &status,
                &generation,
                &checkpoint_sequence,
                &task.priority,
                &available_at,
                &max_attempts,
                &input,
                &deadline,
                &event_id,
                &db_now,
            ],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

fn validate_create_run(command: &CreateRun) -> StoreResult<()> {
    let checkpoint = &command.initial_checkpoint;
    if checkpoint.created_event_id != command.initial_event_id
        || checkpoint.sequence != 1
        || checkpoint.schema_version == 0
        || checkpoint.execution_generation != 0
        || checkpoint.workflow_version_id != command.workflow_version_id
        || checkpoint.coordinator_agent_version_id != command.coordinator_agent_version_id
        || command
            .initial_tasks
            .iter()
            .any(|task| task.max_attempts == 0)
    {
        return Err(invalid_command(
            "create_run initial event/checkpoint/task shape is invalid",
        ));
    }
    Ok(())
}

fn map_completion_shape(error: CompletionShapeError) -> StoreError {
    invalid_command(&format!("invalid complete_task shape: {error:?}"))
}

#[allow(clippy::needless_pass_by_value)]
fn map_database_error(error: tokio_postgres::Error) -> StoreError {
    let code = error.as_db_error().map(|value| value.code().code());
    match code {
        Some("40001" | "40P01") => StoreError::new(
            StoreErrorCode::SerializationConflict,
            agent_loom_durable_store::RetryClass::Backoff,
            "database transaction must be retried",
        ),
        Some(value) if value.starts_with("23") => StoreError::new(
            StoreErrorCode::ConstraintViolation,
            agent_loom_durable_store::RetryClass::Never,
            "database constraint rejected the command",
        ),
        Some("55P03" | "57014") => StoreError::new(
            StoreErrorCode::StoreUnavailable,
            agent_loom_durable_store::RetryClass::Backoff,
            "database lock or statement timeout",
        ),
        _ => StoreError::new(
            StoreErrorCode::StoreUnavailable,
            agent_loom_durable_store::RetryClass::Backoff,
            "PostgreSQL operation failed",
        ),
    }
}

fn store_error(code: StoreErrorCode, message: &str) -> StoreError {
    let retry = match code {
        StoreErrorCode::VersionConflict | StoreErrorCode::LeaseLost => {
            agent_loom_durable_store::RetryClass::ReloadState
        }
        _ => agent_loom_durable_store::RetryClass::Never,
    };
    StoreError::new(code, retry, message)
}

fn invalid_command(message: &str) -> StoreError {
    store_error(StoreErrorCode::ConstraintViolation, message)
}

fn inconsistent(message: &str) -> StoreError {
    store_error(StoreErrorCode::InconsistentProjection, message)
}

fn json_value(payload: &JsonPayload) -> StoreResult<Value> {
    serde_json::from_slice(payload.as_bytes())
        .map_err(|_| invalid_command("payload is not valid JSON"))
}

fn to_i64(value: u64, field: &str) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| invalid_command(&format!("{field} exceeds database range")))
}

fn nonnegative_u64(value: i64, field: &str) -> StoreResult<u64> {
    u64::try_from(value).map_err(|_| inconsistent(&format!("database {field} is negative")))
}

fn positive_u32(value: i64, field: &str) -> StoreResult<u32> {
    let converted = u32::try_from(value)
        .map_err(|_| inconsistent(&format!("database {field} is outside u32 range")))?;
    if converted == 0 {
        return Err(inconsistent(&format!("database {field} is zero")));
    }
    Ok(converted)
}

fn uuid(bytes: [u8; 16]) -> Uuid {
    Uuid::from_bytes(bytes)
}

fn run_id_from_uuid(value: Uuid) -> RunId {
    RunId::from_bytes(value.into_bytes())
}

fn task_id_from_uuid(value: Uuid) -> TaskId {
    TaskId::from_bytes(value.into_bytes())
}

fn stage_id_from_uuid(value: Uuid) -> StageExecutionId {
    StageExecutionId::from_bytes(value.into_bytes())
}

fn workflow_id_from_uuid(value: Uuid) -> WorkflowVersionId {
    WorkflowVersionId::from_bytes(value.into_bytes())
}

fn event_id_from_uuid(value: Uuid) -> EventId {
    EventId::from_bytes(value.into_bytes())
}

fn event_id(value: Uuid) -> EventId {
    event_id_from_uuid(value)
}

fn deterministic_attempt_id(task_id: Uuid, attempt: i64, token: &[u8]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(task_id.as_bytes());
    hasher.update(attempt.to_be_bytes());
    hasher.update(token);
    let digest: [u8; 32] = hasher.finalize().into();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

const fn task_kind(kind: TaskKind) -> &'static str {
    match kind {
        TaskKind::Model => "model",
        TaskKind::Tool => "tool",
        TaskKind::AgentServer => "agent_server",
        TaskKind::ArtifactCheck => "artifact_check",
        TaskKind::TimerWakeup => "timer_wakeup",
        TaskKind::Reconcile => "reconcile",
        TaskKind::StopExternalExecution => "stop_external_execution",
    }
}

fn parse_task_kind(value: &str) -> StoreResult<TaskKind> {
    match value {
        "model" => Ok(TaskKind::Model),
        "tool" => Ok(TaskKind::Tool),
        "agent_server" => Ok(TaskKind::AgentServer),
        "artifact_check" => Ok(TaskKind::ArtifactCheck),
        "timer_wakeup" => Ok(TaskKind::TimerWakeup),
        "reconcile" => Ok(TaskKind::Reconcile),
        "stop_external_execution" => Ok(TaskKind::StopExternalExecution),
        _ => Err(inconsistent("database contains an unknown task kind")),
    }
}

const fn stage_status(status: StageStatus) -> &'static str {
    match status {
        StageStatus::Planned => "planned",
        StageStatus::Active => "active",
        StageStatus::WaitingApproval => "waiting_approval",
        StageStatus::ReworkRequired => "rework_required",
        StageStatus::Succeeded => "succeeded",
        StageStatus::Failed => "failed",
        StageStatus::Skipped => "skipped",
        StageStatus::Cancelled => "cancelled",
    }
}

const fn run_status(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::Waiting => "waiting",
        RunStatus::ApprovalRequired => "approval_required",
        RunStatus::Retrying => "retrying",
        RunStatus::Paused => "paused",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
        RunStatus::TimedOut => "timed_out",
    }
}

fn parse_run_status(value: &str) -> StoreResult<RunStatus> {
    match value {
        "queued" => Ok(RunStatus::Queued),
        "running" => Ok(RunStatus::Running),
        "waiting" => Ok(RunStatus::Waiting),
        "approval_required" => Ok(RunStatus::ApprovalRequired),
        "retrying" => Ok(RunStatus::Retrying),
        "paused" => Ok(RunStatus::Paused),
        "completed" => Ok(RunStatus::Completed),
        "failed" => Ok(RunStatus::Failed),
        "cancelled" => Ok(RunStatus::Cancelled),
        "timed_out" => Ok(RunStatus::TimedOut),
        _ => Err(inconsistent("database contains an unknown run status")),
    }
}

fn committed_run(
    disposition: CommandDisposition,
    snapshot: RunSnapshot,
    event_id: Option<EventId>,
    durable_follow_ups: Vec<DurableFollowUp>,
) -> Committed<RunSnapshot> {
    let run_id = snapshot.run_id;
    Committed {
        disposition,
        value: snapshot,
        event_ids: event_id.into_iter().collect(),
        durable_follow_ups,
        post_commit_hints: vec![
            PostCommitHint::RunEventsAvailable { run_id },
            PostCommitHint::InvalidateRunCache { run_id },
            PostCommitHint::WakeWorkers,
        ],
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RunReceipt {
    #[serde(rename = "type")]
    outcome_type: String,
    run_id: Uuid,
    workflow_version_id: Option<Uuid>,
    status: String,
    suspended_from_status: Option<String>,
    version: u64,
    execution_generation: u64,
    next_event_sequence: u64,
    current_checkpoint_id: Option<Uuid>,
    terminal_event_id: Option<Uuid>,
    deadline: Option<i64>,
    updated_at: i64,
}

fn encode_run_receipt(snapshot: &RunSnapshot) -> StoreResult<Value> {
    serde_json::to_value(RunReceipt {
        outcome_type: "run".to_owned(),
        run_id: uuid(snapshot.run_id.into_bytes()),
        workflow_version_id: snapshot.workflow_version_id.map(|id| uuid(id.into_bytes())),
        status: run_status(snapshot.status).to_owned(),
        suspended_from_status: snapshot
            .suspended_from_status
            .map(run_status)
            .map(str::to_owned),
        version: snapshot.version,
        execution_generation: snapshot.execution_generation,
        next_event_sequence: snapshot.next_event_sequence,
        current_checkpoint_id: snapshot
            .current_checkpoint_id
            .map(|id| uuid(id.into_bytes())),
        terminal_event_id: snapshot.terminal_event_id.map(|id| uuid(id.into_bytes())),
        deadline: snapshot.deadline.map(UnixMicros::get),
        updated_at: snapshot.updated_at.get(),
    })
    .map_err(|_| inconsistent("failed to encode command receipt"))
}

fn decode_run_receipt(tenant_id: TenantId, value: &Value) -> StoreResult<RunSnapshot> {
    let receipt: RunReceipt = serde_json::from_value(value.clone())
        .map_err(|_| inconsistent("stored command receipt is not a Run outcome"))?;
    if receipt.outcome_type != "run" {
        return Err(inconsistent(
            "stored command receipt has the wrong outcome type",
        ));
    }
    Ok(RunSnapshot {
        tenant_id,
        run_id: run_id_from_uuid(receipt.run_id),
        workflow_version_id: receipt.workflow_version_id.map(workflow_id_from_uuid),
        status: parse_run_status(&receipt.status)?,
        suspended_from_status: receipt
            .suspended_from_status
            .as_deref()
            .map(parse_run_status)
            .transpose()?,
        version: receipt.version,
        execution_generation: receipt.execution_generation,
        next_event_sequence: receipt.next_event_sequence,
        current_checkpoint_id: receipt
            .current_checkpoint_id
            .map(|id| CheckpointId::from_bytes(id.into_bytes())),
        terminal_event_id: receipt.terminal_event_id.map(event_id_from_uuid),
        deadline: receipt.deadline.map(UnixMicros::new),
        updated_at: UnixMicros::new(receipt.updated_at),
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct ClaimReceipt {
    #[serde(rename = "type")]
    outcome_type: String,
    task_id: Uuid,
    run_id: Uuid,
    stage_execution_id: Option<Uuid>,
    logical_key: String,
    kind: String,
    generation: u64,
    attempt: u32,
    max_attempts: u32,
    available_at: i64,
    lease_expires_at: i64,
}

fn encode_claim_receipt(value: Option<&ClaimedTask>) -> StoreResult<Value> {
    let Some(value) = value else {
        return Ok(json!({"type": "claim_none"}));
    };
    serde_json::to_value(ClaimReceipt {
        outcome_type: "claim".to_owned(),
        task_id: uuid(value.task.task_id.into_bytes()),
        run_id: uuid(value.task.run_id.into_bytes()),
        stage_execution_id: value
            .task
            .stage_execution_id
            .map(|id| uuid(id.into_bytes())),
        logical_key: value.task.logical_key.as_str().to_owned(),
        kind: task_kind(value.task.kind).to_owned(),
        generation: value.task.generation,
        attempt: value.task.attempt,
        max_attempts: value.task.max_attempts,
        available_at: value.task.available_at.get(),
        lease_expires_at: value.lease_expires_at.get(),
    })
    .map_err(|_| inconsistent("failed to encode claim receipt"))
}

fn decode_claim_receipt(tenant_id: TenantId, value: &Value) -> StoreResult<Option<ClaimedTask>> {
    if value.get("type").and_then(Value::as_str) == Some("claim_none") {
        return Ok(None);
    }
    let receipt: ClaimReceipt = serde_json::from_value(value.clone())
        .map_err(|_| inconsistent("stored command receipt is not a claim outcome"))?;
    if receipt.outcome_type != "claim" {
        return Err(inconsistent(
            "stored command receipt has the wrong outcome type",
        ));
    }
    Ok(Some(ClaimedTask {
        task: TaskSnapshot {
            tenant_id,
            task_id: task_id_from_uuid(receipt.task_id),
            run_id: run_id_from_uuid(receipt.run_id),
            stage_execution_id: receipt.stage_execution_id.map(stage_id_from_uuid),
            logical_key: LogicalKey::parse(receipt.logical_key)
                .map_err(|_| inconsistent("claim receipt contains an invalid logical key"))?,
            kind: parse_task_kind(&receipt.kind)?,
            status: TaskStatus::Leased,
            generation: receipt.generation,
            attempt: receipt.attempt,
            max_attempts: receipt.max_attempts,
            available_at: UnixMicros::new(receipt.available_at),
        },
        lease_expires_at: UnixMicros::new(receipt.lease_expires_at),
    }))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use agent_loom_domain::{
        CommandId, CorrelationId, Digest, DurationMicros, IdempotencyKey, LeaseToken, ScopeKey,
        WorkerId,
    };
    use agent_loom_durable_store::{
        ExpectedRun, FinalRunResult, LeaseProof, NewCheckpoint, TaskResult,
    };

    use super::*;
    use crate::PostgresMigrationExecutor;

    #[test]
    fn task_kind_mapping_matches_schema_contract() {
        let kinds = [
            TaskKind::Model,
            TaskKind::Tool,
            TaskKind::AgentServer,
            TaskKind::ArtifactCheck,
            TaskKind::TimerWakeup,
            TaskKind::Reconcile,
            TaskKind::StopExternalExecution,
        ];
        for kind in kinds {
            assert_eq!(parse_task_kind(task_kind(kind)), Ok(kind));
        }
    }

    #[test]
    fn deterministic_attempt_identifier_is_stable_and_lease_scoped() {
        let task = Uuid::from_bytes([1; 16]);
        let first = deterministic_attempt_id(task, 1, &[2; 32]);
        assert_eq!(first, deterministic_attempt_id(task, 1, &[2; 32]));
        assert_ne!(first, deterministic_attempt_id(task, 2, &[2; 32]));
        assert_ne!(first, deterministic_attempt_id(task, 1, &[3; 32]));
    }

    #[test]
    fn receipt_round_trip_preserves_original_run_snapshot() {
        let snapshot = RunSnapshot {
            tenant_id: TenantId::from_bytes([1; 16]),
            run_id: RunId::from_bytes([2; 16]),
            workflow_version_id: Some(WorkflowVersionId::from_bytes([3; 16])),
            status: RunStatus::Completed,
            suspended_from_status: None,
            version: 9,
            execution_generation: 2,
            next_event_sequence: 11,
            current_checkpoint_id: Some(CheckpointId::from_bytes([4; 16])),
            terminal_event_id: Some(EventId::from_bytes([5; 16])),
            deadline: Some(UnixMicros::new(99)),
            updated_at: UnixMicros::new(100),
        };
        let encoded = encode_run_receipt(&snapshot).expect("encode");
        let decoded = decode_run_receipt(snapshot.tenant_id, &encoded).expect("decode");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn transaction_sql_contains_required_concurrency_guards() {
        let source = include_str!("transaction_executor.rs");
        assert!(source.contains("FOR UPDATE OF t, r SKIP LOCKED"));
        assert!(source.contains("ON CONFLICT (tenant_id, scope, idempotency_key) DO NOTHING"));
        assert!(source.contains("AND execution_generation = $4"));
        assert!(source.contains("AND status IN ('queued', 'running', 'waiting'"));
        assert!(source.contains("AND outcome_kind = 'outcome_unknown'"));
        assert!(source.contains("lease_expires_at.is_none_or(|expires| expires <= db_now)"));
        let legacy_to_timestamp = ["micros", "_to_", "timestamptz"].concat();
        let legacy_from_timestamp = ["timestamptz", "_to_", "micros"].concat();
        assert!(!source.contains(&legacy_to_timestamp));
        assert!(!source.contains(&legacy_from_timestamp));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn transaction_vertical_slice_when_postgres_url_is_configured() {
        let Ok(url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
            return;
        };
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect to smoke-test PostgreSQL");
        let connection_task = tokio::spawn(connection);
        PostgresMigrationExecutor::new(
            &mut client,
            &crate::PostgresConfig::default(),
            env!("CARGO_PKG_VERSION"),
        )
        .migrate()
        .await
        .expect("migrate smoke-test PostgreSQL");

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let ids = |offset: u128| nonce.wrapping_add(offset).to_be_bytes();
        let tenant_id = TenantId::from_bytes(ids(1));
        let tenant_uuid = uuid(tenant_id.into_bytes());
        let tenant_key = format!("smoke-{tenant_uuid}");
        client
            .execute(
                "INSERT INTO agent_loom.tenants (\
                    tenant_id, tenant_key, status, policy_json, created_at, updated_at\
                 ) VALUES ($1, $2, 'active', '{}'::jsonb, clock_timestamp(), clock_timestamp())",
                &[&tenant_uuid, &tenant_key],
            )
            .await
            .expect("insert smoke tenant");

        let run_id = RunId::from_bytes(ids(2));
        let task_id = TaskId::from_bytes(ids(3));
        let initial_event_id = EventId::from_bytes(ids(4));
        let checkpoint_id = CheckpointId::from_bytes(ids(5));
        let now_micros = i64::try_from(nonce / 1_000).expect("current micros fit i64");
        let create_context = command_context(tenant_id, ids(6), "create_run", &tenant_key, 6);
        let create = CreateRun {
            run_id,
            workflow_version_id: None,
            coordinator_agent_version_id: None,
            input: payload(&json!({"request": "smoke"})),
            deadline: None,
            initial_event_id,
            initial_checkpoint: NewCheckpoint {
                checkpoint_id,
                sequence: 1,
                schema_version: 1,
                workflow_version_id: None,
                coordinator_agent_version_id: None,
                execution_generation: 0,
                state: payload(&json!({"step": 1})),
                state_digest: Digest::from_bytes([5; 32]),
                created_event_id: initial_event_id,
            },
            initial_tasks: vec![InitialTask {
                task_id,
                stage_execution_id: None,
                logical_key: LogicalKey::parse("smoke/task").expect("logical key"),
                kind: TaskKind::Model,
                priority: 10,
                available_at: UnixMicros::new(now_micros),
                max_attempts: 3,
                input: payload(&json!({"prompt": "smoke"})),
            }],
        };
        let executor = PostgresTransactionExecutor::default();
        let created = executor
            .create_run(&mut client, &create_context, create.clone())
            .await
            .expect("create run atomically");
        assert_eq!(created.disposition, CommandDisposition::Applied);
        let duplicate = executor
            .create_run(&mut client, &create_context, create)
            .await
            .expect("replay create run");
        assert_eq!(duplicate.disposition, CommandDisposition::Duplicate);
        assert_eq!(duplicate.value, created.value);

        let worker_id = WorkerId::from_bytes(ids(7));
        let lease_token = LeaseToken::from_bytes([8; 32]);
        let claim_context = command_context(tenant_id, ids(8), "claim_task", &tenant_key, 8);
        let claimed = executor
            .claim_task(
                &mut client,
                &claim_context,
                ClaimTask {
                    worker_id,
                    lease_token: lease_token.clone(),
                    lease_duration: DurationMicros::new(60_000_000),
                    candidate_window: 8,
                },
            )
            .await
            .expect("claim task atomically")
            .expect("one task is claimable");
        assert_eq!(claimed.value.task.task_id, task_id);

        let completion_event_id = EventId::from_bytes(ids(9));
        let complete_context =
            command_context(tenant_id, ids(10), "complete_task", &tenant_key, 10);
        let completed = executor
            .complete_task(
                &mut client,
                &complete_context,
                CompleteTask {
                    expected_run: ExpectedRun {
                        run_id,
                        version: Some(1),
                        execution_generation: Some(0),
                    },
                    lease: LeaseProof {
                        task_id,
                        worker_id,
                        token: lease_token,
                        execution_generation: 0,
                    },
                    completion_event_id,
                    checkpoint: NewCheckpoint {
                        checkpoint_id: CheckpointId::from_bytes(ids(11)),
                        sequence: 2,
                        schema_version: 1,
                        workflow_version_id: None,
                        coordinator_agent_version_id: None,
                        execution_generation: 0,
                        state: payload(&json!({"step": 2})),
                        state_digest: Digest::from_bytes([11; 32]),
                        created_event_id: completion_event_id,
                    },
                    task_result: TaskResult {
                        output: payload(&json!({"ok": true})),
                    },
                    stage_mutation: None,
                    artifacts: Vec::new(),
                    next: NextActions::FinishRun(FinalRunResult {
                        status: RunStatus::Completed,
                        output: payload(&json!({"delivered": true})),
                    }),
                },
            )
            .await
            .expect("complete task atomically");
        assert_eq!(completed.value.status, RunStatus::Completed);
        assert!(completed.value.terminal_invariant_holds());

        drop(client);
        connection_task
            .await
            .expect("connection task joins")
            .expect("PostgreSQL connection stays healthy");
    }

    fn payload(value: &Value) -> JsonPayload {
        JsonPayload::from_validated_bytes(serde_json::to_vec(&value).expect("serialize payload"))
    }

    fn command_context(
        tenant_id: TenantId,
        command_bytes: [u8; 16],
        scope: &str,
        key_suffix: &str,
        digest: u8,
    ) -> CommandContext {
        CommandContext {
            tenant_id,
            command_id: CommandId::from_bytes(command_bytes),
            correlation_id: CorrelationId::from_bytes(command_bytes),
            actor_ref: "postgres-smoke-test".to_owned(),
            scope: ScopeKey::parse(scope).expect("scope"),
            idempotency_key: IdempotencyKey::parse(format!("{scope}-{key_suffix}"))
                .expect("idempotency key"),
            request_hash: Digest::from_bytes([digest; 32]),
        }
    }
}
