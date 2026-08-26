use agent_loom_domain::{
    AgentExecutionId, CheckpointId, EventId, JsonPayload, LogicalKey, RunId, RunSnapshot,
    RunStatus, StageExecutionId, StageStatus, TaskId, TaskKind, TaskSnapshot, TaskStatus, TenantId,
    ToolExecutionId, ToolExecutionSnapshot, ToolExecutionStatus, UnixMicros, WorkflowVersionId,
};
use agent_loom_durable_store::{
    ApplyEvent, ClaimTask, ClaimedTask, CommandContext, CommandDisposition, Committed,
    CompleteTask, CompletionShapeError, ControlRun, CreateRun, DurableFollowUp,
    ExecutionRetryClass, ExpectedRun, FailTask, InitialTask, LeaseProof, NewTask, NextActions,
    PostCommitHint, PrepareToolExecution, RecordToolOutcome, RenewTaskLease, SignatureVerification,
    StoreError, StoreErrorCode, StoreResult, ToolRecordedOutcome,
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
                payload_schema_version: 1,
                producer: "runtime",
                context,
                correlation_id,
                occurred_at: None,
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
        let candidate =
            lock_claim_candidate(&transaction, tenant_id, db_now, command.candidate_window).await?;

        let Some(row) = candidate else {
            let outcome = json!({"type": "claim_none"});
            finish_receipt(&transaction, context, "no_op", &outcome, None, None).await?;
            transaction.commit().await.map_err(map_database_error)?;
            return Ok(None);
        };

        let task_id = row.task_id;
        let run_id = row.run_id;
        let stage_execution_id = row.stage_execution_id;
        let generation = nonnegative_u64(row.generation, "task generation")?;
        let max_attempts = positive_u32(row.max_attempts, "task max attempts")?;
        let attempt = row
            .attempt
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
                sequence: row.next_event_sequence,
                event_type: "task.claimed",
                payload: &event_payload,
                payload_schema_version: 1,
                producer: "worker",
                context,
                correlation_id,
                occurred_at: None,
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
                logical_key: LogicalKey::parse(row.logical_key)
                    .map_err(|_| inconsistent("database contains an invalid task logical key"))?,
                kind: parse_task_kind(&row.kind)?,
                status: TaskStatus::Leased,
                generation,
                attempt: positive_u32(attempt, "task attempt")?,
                max_attempts,
                available_at: UnixMicros::new(row.available_at),
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

    /// Extends an active Task lease and its matching attempt using the
    /// authoritative database clock. The Run version is intentionally not
    /// advanced because renewal does not change the workflow projection.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for an invalid extension, stale Run fence,
    /// lost or expired lease, idempotency misuse, or database failure.
    #[allow(clippy::too_many_lines)]
    pub async fn renew_task_lease(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: RenewTaskLease,
    ) -> StoreResult<Committed<ClaimedTask>> {
        if command.extension.get() == 0 {
            return Err(invalid_command("lease extension must be positive"));
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
                let value = decode_claim_receipt(context.tenant_id, &receipt.outcome)?
                    .ok_or_else(|| inconsistent("renewal receipt has no claimed task"))?;
                transaction.commit().await.map_err(map_database_error)?;
                return Ok(Committed {
                    disposition: CommandDisposition::Duplicate,
                    value,
                    event_ids: receipt.event_id.map(event_id).into_iter().collect(),
                    durable_follow_ups: Vec::new(),
                    post_commit_hints: Vec::new(),
                });
            }
            ReceiptGuard::Acquired => {}
        }

        let tenant_id = uuid(context.tenant_id.into_bytes());
        let locked = lock_worker_task(
            &transaction,
            tenant_id,
            uuid(command.expected_run.run_id.into_bytes()),
            uuid(command.lease.task_id.into_bytes()),
        )
        .await?;
        validate_worker_fences(
            &command.expected_run,
            &command.lease,
            &locked,
            db_now,
            "renewal",
        )?;
        let extension = to_i64(command.extension.get(), "lease extension")?;
        let lease_expires_at = locked
            .lease_expires_at
            .and_then(|expires| expires.checked_add(extension))
            .ok_or_else(|| invalid_command("lease expiry overflow"))?;
        let lease_token = command.lease.token.as_bytes().as_slice();
        let task_updated = transaction
            .execute(
                "UPDATE agent_loom.tasks SET lease_expires_at = \
                    to_timestamp(($6::bigint)::double precision / 1000000.0), \
                    updated_at = to_timestamp(($7::bigint)::double precision / 1000000.0) \
                 WHERE tenant_id = $1 AND run_id = $2 AND task_id = $3 \
                   AND status = 'leased' AND lease_owner = $4 AND lease_token = $5",
                &[
                    &tenant_id,
                    &locked.run_id,
                    &locked.task_id,
                    &locked.lease_owner,
                    &lease_token,
                    &lease_expires_at,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;
        if task_updated != 1 {
            return Err(store_error(
                StoreErrorCode::LeaseLost,
                "task lease changed during renewal",
            ));
        }
        let attempt_updated = transaction
            .execute(
                "UPDATE agent_loom.task_attempts SET lease_expires_at = \
                    to_timestamp(($4::bigint)::double precision / 1000000.0) \
                 WHERE tenant_id = $1 AND task_id = $2 AND attempt = $3 \
                   AND finished_at IS NULL",
                &[
                    &tenant_id,
                    &locked.task_id,
                    &locked.attempt,
                    &lease_expires_at,
                ],
            )
            .await
            .map_err(map_database_error)?;
        if attempt_updated != 1 {
            return Err(inconsistent("leased task has no open matching attempt"));
        }

        let value = locked.claimed_task(context.tenant_id, lease_expires_at)?;
        let outcome = encode_claim_receipt(Some(&value))?;
        finish_receipt(
            &transaction,
            context,
            "applied",
            &outcome,
            None,
            Some(("task", locked.task_id, locked.attempt)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;
        Ok(Committed {
            disposition: CommandDisposition::Applied,
            value,
            event_ids: Vec::new(),
            durable_follow_ups: Vec::new(),
            post_commit_hints: Vec::new(),
        })
    }

    /// Finalizes the active Task attempt and atomically projects the Run to a
    /// retry, fatal failure, or dead-letter/manual-recovery state.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for invalid failure metadata, stale Run
    /// expectations, lost or expired lease, idempotency misuse, or database failure.
    #[allow(clippy::too_many_lines)]
    pub async fn fail_task(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: FailTask,
    ) -> StoreResult<Committed<RunSnapshot>> {
        if command.error_code.is_empty() {
            return Err(invalid_command("task failure error code must not be empty"));
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
                let snapshot = decode_run_receipt(context.tenant_id, &receipt.outcome)?;
                transaction.commit().await.map_err(map_database_error)?;
                return Ok(failure_committed(
                    CommandDisposition::Duplicate,
                    snapshot,
                    receipt.event_id.map(event_id),
                    Vec::new(),
                    false,
                ));
            }
            ReceiptGuard::Acquired => {}
        }

        let tenant_id = uuid(context.tenant_id.into_bytes());
        let locked = lock_worker_task(
            &transaction,
            tenant_id,
            uuid(command.expected_run.run_id.into_bytes()),
            uuid(command.lease.task_id.into_bytes()),
        )
        .await?;
        validate_worker_fences(
            &command.expected_run,
            &command.lease,
            &locked,
            db_now,
            "failure",
        )?;
        let transition = FailureTransition::classify(
            locked.attempt,
            locked.max_attempts,
            command.retry_at.map(UnixMicros::get),
        );
        let event_id = uuid(command.failure_event_id.into_bytes());
        let correlation_id = uuid(context.correlation_id.into_bytes());
        let event_payload = json!({
            "task_id": locked.task_id,
            "attempt": locked.attempt,
            "max_attempts": locked.max_attempts,
            "error_code": &command.error_code,
            "task_status": transition.task_status,
            "run_status": transition.run_status,
            "retry_at_micros": transition.retry_at,
        });
        insert_event(
            &transaction,
            EventInsert {
                event_id,
                tenant_id,
                run_id: locked.run_id,
                sequence: locked.next_event_sequence,
                event_type: transition.event_type,
                payload: &event_payload,
                payload_schema_version: 1,
                producer: "worker",
                context,
                correlation_id,
                occurred_at: None,
                recorded_at: db_now,
            },
        )
        .await?;
        finalize_failed_task(
            &transaction,
            tenant_id,
            &locked,
            db_now,
            &command.error_code,
            &event_payload,
            &transition,
        )
        .await?;
        let mut follow_ups = Vec::new();
        if transition.terminal {
            close_work_after_fatal_failure(
                &transaction,
                tenant_id,
                locked.run_id,
                locked.task_id,
                db_now,
            )
            .await?;
            let (_, mut stop_follow_ups) =
                request_external_stops(&transaction, tenant_id, locked.run_id, db_now).await?;
            follow_ups.append(&mut stop_follow_ups);
            let mut reconcile_follow_ups =
                mark_executing_tools_uncertain(&transaction, tenant_id, locked.run_id, db_now)
                    .await?;
            follow_ups.append(&mut reconcile_follow_ups);
        }

        let next_version = locked
            .run_version
            .checked_add(1)
            .ok_or_else(|| inconsistent("run version overflow"))?;
        let next_event_sequence = locked
            .next_event_sequence
            .checked_add(1)
            .ok_or_else(|| inconsistent("run event sequence overflow"))?;
        let run_updated = transaction
            .execute(
                "UPDATE agent_loom.runs SET status = $5, suspended_from_status = NULL, \
                    version = $6, next_event_sequence = $7, \
                    terminal_event_id = CASE WHEN $8 THEN $9 ELSE NULL END, \
                    terminal_at = CASE WHEN $8 THEN \
                        to_timestamp(($10::bigint)::double precision / 1000000.0) ELSE NULL END, \
                    updated_at = to_timestamp(($10::bigint)::double precision / 1000000.0) \
                 WHERE tenant_id = $1 AND run_id = $2 AND version = $3 \
                   AND execution_generation = $4 \
                   AND status IN ('queued', 'running', 'retrying')",
                &[
                    &tenant_id,
                    &locked.run_id,
                    &locked.run_version,
                    &locked.execution_generation,
                    &transition.run_status,
                    &next_version,
                    &next_event_sequence,
                    &transition.terminal,
                    &event_id,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;
        if run_updated != 1 {
            return Err(store_error(
                StoreErrorCode::VersionConflict,
                "run changed while failing the task",
            ));
        }

        let snapshot = RunSnapshot {
            tenant_id: context.tenant_id,
            run_id: run_id_from_uuid(locked.run_id),
            workflow_version_id: locked.workflow_version_id.map(workflow_id_from_uuid),
            status: parse_run_status(transition.run_status)?,
            suspended_from_status: None,
            version: nonnegative_u64(next_version, "run version")?,
            execution_generation: nonnegative_u64(
                locked.execution_generation,
                "execution generation",
            )?,
            next_event_sequence: nonnegative_u64(next_event_sequence, "event sequence")?,
            current_checkpoint_id: locked
                .current_checkpoint_id
                .map(|id| CheckpointId::from_bytes(id.into_bytes())),
            terminal_event_id: transition.terminal.then_some(command.failure_event_id),
            deadline: locked.deadline.map(UnixMicros::new),
            updated_at: UnixMicros::new(db_now),
        };
        if let Some(not_before) = transition.retry_at {
            follow_ups.push(DurableFollowUp::ScanDueWork {
                not_before: UnixMicros::new(not_before),
            });
        }
        let outcome = encode_run_receipt(&snapshot)?;
        finish_receipt(
            &transaction,
            context,
            "applied",
            &outcome,
            Some(event_id),
            Some(("run", locked.run_id, next_version)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;
        Ok(failure_committed(
            CommandDisposition::Applied,
            snapshot,
            Some(command.failure_event_id),
            follow_ups,
            transition.retry_at.is_some(),
        ))
    }

    /// Matches and consumes one Wait, appends the external Event, creates the
    /// persisted resume Task, and advances the Run projection in one transaction.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for failed signature verification, stale
    /// expectations, missing/ambiguous/consumed/expired waits, terminal Runs,
    /// idempotency misuse, or database failure.
    #[allow(clippy::too_many_lines)]
    pub async fn apply_event(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: ApplyEvent,
    ) -> StoreResult<Committed<RunSnapshot>> {
        validate_apply_event(&command)?;
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
                return Ok(event_committed(
                    CommandDisposition::Duplicate,
                    snapshot,
                    receipt.event_id.map(event_id),
                    None,
                ));
            }
            ReceiptGuard::Acquired => {}
        }

        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_id = uuid(command.expected_run.run_id.into_bytes());
        let match_hash = command.match_key_hash.as_bytes().as_slice();
        let candidate = find_wait_candidate(
            &transaction,
            tenant_id,
            run_id,
            &command.event_type,
            match_hash,
        )
        .await?;
        let locked_run = lock_event_run(&transaction, tenant_id, run_id).await?;
        validate_event_run(&command.expected_run, &locked_run, db_now)?;
        let Some(candidate) = candidate else {
            return Err(classify_wait_miss(
                &transaction,
                tenant_id,
                run_id,
                &command.event_type,
                match_hash,
                db_now,
            )
            .await?);
        };
        if let Some(stage_id) = candidate.stage_execution_id {
            lock_event_stage(&transaction, tenant_id, run_id, stage_id).await?;
        }
        let wait = lock_wait(&transaction, tenant_id, run_id, candidate.wait_id).await?;
        validate_locked_wait(&command, &wait, db_now)?;

        let event_id = uuid(command.event_id.into_bytes());
        let correlation_id = uuid(context.correlation_id.into_bytes());
        let payload = json_value(&command.payload)?;
        let payload_schema_version = i64::from(command.payload_schema_version);
        let producer = match command.signature_verification {
            SignatureVerification::Verified => "external-verified",
            SignatureVerification::NotRequired => "external-trusted",
            SignatureVerification::Failed => unreachable!("validated above"),
        };
        insert_event(
            &transaction,
            EventInsert {
                event_id,
                tenant_id,
                run_id,
                sequence: locked_run.next_event_sequence,
                event_type: &command.event_type,
                payload: &payload,
                payload_schema_version,
                producer,
                context,
                correlation_id,
                occurred_at: command.occurred_at.map(UnixMicros::get),
                recorded_at: db_now,
            },
        )
        .await?;
        consume_wait(&transaction, tenant_id, wait.wait_id, event_id, db_now).await?;
        reactivate_waiting_stage(
            &transaction,
            tenant_id,
            run_id,
            wait.stage_execution_id,
            &wait.wait_type,
            db_now,
        )
        .await?;
        let checkpoint_sequence = locked_run
            .checkpoint_sequence
            .ok_or_else(|| inconsistent("waiting run has no current checkpoint"))?;
        let paused = locked_run.status == "paused";
        insert_wait_resume_task(
            &transaction,
            tenant_id,
            run_id,
            event_id,
            db_now,
            locked_run.execution_generation,
            checkpoint_sequence,
            &wait,
            &command,
            paused,
        )
        .await?;
        let target_status = if paused {
            "paused"
        } else if run_has_leased_task(&transaction, tenant_id, run_id).await? {
            "running"
        } else {
            "queued"
        };
        let next_version = locked_run
            .version
            .checked_add(1)
            .ok_or_else(|| inconsistent("run version overflow"))?;
        let next_event_sequence = locked_run
            .next_event_sequence
            .checked_add(1)
            .ok_or_else(|| inconsistent("run event sequence overflow"))?;
        let updated = transaction
            .execute(
                "UPDATE agent_loom.runs SET status = $5, \
                    suspended_from_status = CASE WHEN $5 = 'paused' \
                        THEN suspended_from_status ELSE NULL END, \
                    version = $6, next_event_sequence = $7, \
                    updated_at = to_timestamp(($8::bigint)::double precision / 1000000.0) \
                 WHERE tenant_id = $1 AND run_id = $2 AND version = $3 \
                   AND execution_generation = $4 \
                   AND status NOT IN ('completed', 'failed', 'cancelled', 'timed_out')",
                &[
                    &tenant_id,
                    &run_id,
                    &locked_run.version,
                    &locked_run.execution_generation,
                    &target_status,
                    &next_version,
                    &next_event_sequence,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;
        if updated != 1 {
            return Err(store_error(
                StoreErrorCode::VersionConflict,
                "run changed while applying the event",
            ));
        }

        let snapshot = RunSnapshot {
            tenant_id: context.tenant_id,
            run_id: command.expected_run.run_id,
            workflow_version_id: locked_run.workflow_version_id.map(workflow_id_from_uuid),
            status: parse_run_status(target_status)?,
            suspended_from_status: if paused {
                locked_run
                    .suspended_from_status
                    .as_deref()
                    .map(parse_run_status)
                    .transpose()?
            } else {
                None
            },
            version: nonnegative_u64(next_version, "run version")?,
            execution_generation: nonnegative_u64(
                locked_run.execution_generation,
                "execution generation",
            )?,
            next_event_sequence: nonnegative_u64(next_event_sequence, "event sequence")?,
            current_checkpoint_id: locked_run
                .current_checkpoint_id
                .map(|id| CheckpointId::from_bytes(id.into_bytes())),
            terminal_event_id: None,
            deadline: locked_run.deadline.map(UnixMicros::new),
            updated_at: UnixMicros::new(db_now),
        };
        let outcome = encode_run_receipt(&snapshot)?;
        finish_receipt(
            &transaction,
            context,
            "applied",
            &outcome,
            Some(event_id),
            Some(("run", run_id, next_version)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;
        Ok(event_committed(
            CommandDisposition::Applied,
            snapshot,
            Some(command.event_id),
            (!paused).then_some(wait.resume_task_id),
        ))
    }

    /// Persists Tool execution intent and its first adapter attempt before an
    /// external side effect is performed.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for malformed metadata, a stale/lost
    /// lease, conflicting Tool idempotency identity, or database failure.
    #[allow(clippy::too_many_lines)]
    pub async fn prepare_tool_execution(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: PrepareToolExecution,
    ) -> StoreResult<Committed<ToolExecutionSnapshot>> {
        validate_prepare_tool(&command)?;
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
                let snapshot = decode_tool_receipt(context.tenant_id, &receipt.outcome)?;
                transaction.commit().await.map_err(map_database_error)?;
                return Ok(tool_committed(
                    CommandDisposition::Duplicate,
                    snapshot,
                    receipt.event_id.map(event_id),
                    Vec::new(),
                ));
            }
            ReceiptGuard::Acquired => {}
        }
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let locked = lock_worker_task(
            &transaction,
            tenant_id,
            uuid(command.expected_run.run_id.into_bytes()),
            uuid(command.lease.task_id.into_bytes()),
        )
        .await?;
        validate_worker_fences(
            &command.expected_run,
            &command.lease,
            &locked,
            db_now,
            "tool preparation",
        )?;
        if locked.stage_execution_id
            != command
                .stage_execution_id
                .map(|value| uuid(value.into_bytes()))
        {
            return Err(invalid_command(
                "tool execution stage does not match the leased task",
            ));
        }
        if let Some(existing) = lock_tool_by_idempotency(
            &transaction,
            tenant_id,
            command.idempotency_scope.as_str(),
            command.idempotency_key.as_str(),
        )
        .await?
        {
            validate_existing_tool(&command, &locked, &existing)?;
            let snapshot = existing.snapshot(context.tenant_id)?;
            let outcome = encode_tool_receipt(&snapshot)?;
            finish_receipt(
                &transaction,
                context,
                "no_op",
                &outcome,
                None,
                Some((
                    "tool_execution",
                    existing.tool_execution_id,
                    existing.attempt_count,
                )),
            )
            .await?;
            transaction.commit().await.map_err(map_database_error)?;
            return Ok(tool_committed(
                CommandDisposition::NoOp,
                snapshot,
                None,
                Vec::new(),
            ));
        }
        let execution_id = uuid(command.tool_execution_id.into_bytes());
        let attempt_id = uuid(command.tool_attempt_id.into_bytes());
        let stage_id = command
            .stage_execution_id
            .map(|value| uuid(value.into_bytes()));
        let request = json_value(&command.request)?;
        let request_hash = command.request_hash.as_bytes().as_slice();
        transaction
            .execute(
                "INSERT INTO agent_loom.tool_executions (\
                    tool_execution_id, tenant_id, run_id, stage_execution_id, task_id, \
                    tool_call_id, tool_name, idempotency_scope, idempotency_key, request_hash, \
                    status, attempt_count, request_json, result_json, error_code, \
                    recovery_action, external_ref, started_at, created_at, updated_at, \
                    completed_at, retry_at\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'executing', 1, \
                    $11, NULL, NULL, NULL, NULL, \
                    to_timestamp(($12::bigint)::double precision / 1000000.0), \
                    to_timestamp(($12::bigint)::double precision / 1000000.0), \
                    to_timestamp(($12::bigint)::double precision / 1000000.0), NULL, NULL)",
                &[
                    &execution_id,
                    &tenant_id,
                    &locked.run_id,
                    &stage_id,
                    &locked.task_id,
                    &command.tool_call_id,
                    &command.tool_name,
                    &command.idempotency_scope.as_str(),
                    &command.idempotency_key.as_str(),
                    &request_hash,
                    &request,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;
        transaction
            .execute(
                "INSERT INTO agent_loom.tool_execution_attempts (\
                    tool_attempt_id, tenant_id, tool_execution_id, run_id, attempt, \
                    request_started_at, request_finished_at, adapter_error_code, retry_class, \
                    remote_request_id, external_ref, response_digest, outcome, metrics_json\
                 ) VALUES ($1, $2, $3, $4, 1, \
                    to_timestamp(($5::bigint)::double precision / 1000000.0), \
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{}'::jsonb)",
                &[
                    &attempt_id,
                    &tenant_id,
                    &execution_id,
                    &locked.run_id,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;
        let event_id = uuid(command.prepared_event_id.into_bytes());
        let event_payload = json!({
            "tool_execution_id": execution_id,
            "task_id": locked.task_id,
            "tool_call_id": &command.tool_call_id,
            "tool_name": &command.tool_name,
            "attempt": 1,
        });
        insert_event(
            &transaction,
            EventInsert {
                event_id,
                tenant_id,
                run_id: locked.run_id,
                sequence: locked.next_event_sequence,
                event_type: "tool.execution_prepared",
                payload: &event_payload,
                payload_schema_version: 1,
                producer: "worker",
                context,
                correlation_id: uuid(context.correlation_id.into_bytes()),
                occurred_at: None,
                recorded_at: db_now,
            },
        )
        .await?;
        advance_run_event_cursor(
            &transaction,
            tenant_id,
            locked.run_id,
            locked.run_version,
            locked.execution_generation,
            db_now,
        )
        .await?;
        let snapshot = ToolExecutionSnapshot {
            tenant_id: context.tenant_id,
            tool_execution_id: command.tool_execution_id,
            run_id: run_id_from_uuid(locked.run_id),
            stage_execution_id: command.stage_execution_id,
            task_id: command.lease.task_id,
            tool_call_id: command.tool_call_id,
            tool_name: command.tool_name,
            status: ToolExecutionStatus::Executing,
            attempt_count: 1,
            external_ref: None,
            recovery_action: None,
            retry_at: None,
            updated_at: UnixMicros::new(db_now),
        };
        let outcome = encode_tool_receipt(&snapshot)?;
        finish_receipt(
            &transaction,
            context,
            "applied",
            &outcome,
            Some(event_id),
            Some(("tool_execution", execution_id, 1)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;
        Ok(tool_committed(
            CommandDisposition::Applied,
            snapshot,
            Some(command.prepared_event_id),
            Vec::new(),
        ))
    }

    /// Finalizes one Tool adapter attempt while preserving late or uncertain
    /// external evidence even when the Run generation has already been fenced.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for malformed outcome metadata, a stale
    /// attempt, mismatched execution ownership, idempotency misuse, or database failure.
    #[allow(clippy::too_many_lines)]
    pub async fn record_tool_outcome(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: RecordToolOutcome,
    ) -> StoreResult<Committed<ToolExecutionSnapshot>> {
        let projection = project_tool_outcome(&command.outcome)?;
        if command
            .remote_request_id
            .as_ref()
            .is_some_and(String::is_empty)
        {
            return Err(invalid_command("remote request ID must not be empty"));
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
                let snapshot = decode_tool_receipt(context.tenant_id, &receipt.outcome)?;
                transaction.commit().await.map_err(map_database_error)?;
                return Ok(tool_committed(
                    CommandDisposition::Duplicate,
                    snapshot,
                    receipt.event_id.map(event_id),
                    Vec::new(),
                ));
            }
            ReceiptGuard::Acquired => {}
        }
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_id = uuid(command.expected_run.run_id.into_bytes());
        let run = lock_event_run(&transaction, tenant_id, run_id).await?;
        let task_id = uuid(command.task_id.into_bytes());
        let task = transaction
            .query_opt(
                "SELECT run_id, generation FROM agent_loom.tasks \
                 WHERE tenant_id = $1 AND task_id = $2 FOR UPDATE",
                &[&tenant_id, &task_id],
            )
            .await
            .map_err(map_database_error)?
            .ok_or_else(|| store_error(StoreErrorCode::NotFound, "task was not found"))?;
        let task_run_id: Uuid = task.get(0);
        let task_generation: i64 = task.get(1);
        if task_run_id != run_id {
            return Err(store_error(
                StoreErrorCode::NotFound,
                "tool task belongs to another run",
            ));
        }
        let execution_id = uuid(command.tool_execution_id.into_bytes());
        let execution = lock_tool_by_id(&transaction, tenant_id, execution_id).await?;
        if execution.run_id != run_id || execution.task_id != task_id {
            return Err(store_error(
                StoreErrorCode::NotFound,
                "tool execution belongs to another task or run",
            ));
        }
        if i64::from(command.expected_attempt) != execution.attempt_count {
            return Err(store_error(
                StoreErrorCode::VersionConflict,
                "tool execution attempt changed",
            ));
        }
        if !matches!(execution.status.as_str(), "executing" | "reconciling") {
            return Err(store_error(
                StoreErrorCode::InvalidTransition,
                "tool execution does not accept an adapter outcome",
            ));
        }
        let persisted_external_ref = projection
            .external_ref
            .clone()
            .or_else(|| execution.external_ref.clone());
        let attempt_updated = transaction
            .execute(
                "UPDATE agent_loom.tool_execution_attempts SET \
                    request_finished_at = to_timestamp(($7::bigint)::double precision / 1000000.0), \
                    adapter_error_code = $4, retry_class = $5, remote_request_id = $6, \
                    external_ref = $8, response_digest = $9, outcome = $10 \
                 WHERE tenant_id = $1 AND tool_execution_id = $2 AND attempt = $3 \
                   AND request_finished_at IS NULL",
                &[
                    &tenant_id,
                    &execution_id,
                    &execution.attempt_count,
                    &projection.error_code,
                    &projection.retry_class,
                    &command.remote_request_id,
                    &db_now,
                    &persisted_external_ref,
                    &command.response_digest.map(|value| value.as_bytes().to_vec()),
                    &projection.attempt_outcome,
                ],
            )
            .await
            .map_err(map_database_error)?;
        if attempt_updated != 1 {
            return Err(store_error(
                StoreErrorCode::VersionConflict,
                "tool attempt was already finalized",
            ));
        }
        let terminal = projection.status.is_terminal();
        let status = tool_status(projection.status);
        let updated = transaction
            .execute(
                "UPDATE agent_loom.tool_executions SET status = $3, result_json = $4, \
                    error_code = $5, recovery_action = $6, external_ref = $7, \
                    retry_at = to_timestamp(($8::bigint)::double precision / 1000000.0), \
                    completed_at = CASE WHEN $9 THEN \
                        to_timestamp(($10::bigint)::double precision / 1000000.0) ELSE NULL END, \
                    updated_at = to_timestamp(($10::bigint)::double precision / 1000000.0) \
                 WHERE tenant_id = $1 AND tool_execution_id = $2 \
                   AND status IN ('executing', 'reconciling')",
                &[
                    &tenant_id,
                    &execution_id,
                    &status,
                    &projection.result,
                    &projection.error_code,
                    &projection.recovery_action,
                    &persisted_external_ref,
                    &projection.retry_at,
                    &terminal,
                    &db_now,
                ],
            )
            .await
            .map_err(map_database_error)?;
        if updated != 1 {
            return Err(store_error(
                StoreErrorCode::VersionConflict,
                "tool execution changed while recording outcome",
            ));
        }
        let fenced = i64::try_from(command.execution_generation).ok()
            != Some(run.execution_generation)
            || i64::try_from(command.execution_generation).ok() != Some(task_generation);
        let event_id = uuid(command.outcome_event_id.into_bytes());
        let event_payload = json!({
            "tool_execution_id": execution_id,
            "task_id": task_id,
            "status": status,
            "attempt": execution.attempt_count,
            "fenced": fenced,
            "error_code": &projection.error_code,
            "external_ref": &persisted_external_ref,
        });
        insert_event(
            &transaction,
            EventInsert {
                event_id,
                tenant_id,
                run_id,
                sequence: run.next_event_sequence,
                event_type: projection.event_type,
                payload: &event_payload,
                payload_schema_version: 1,
                producer: "tool-adapter",
                context,
                correlation_id: uuid(context.correlation_id.into_bytes()),
                occurred_at: None,
                recorded_at: db_now,
            },
        )
        .await?;
        advance_run_event_cursor(
            &transaction,
            tenant_id,
            run_id,
            run.version,
            run.execution_generation,
            db_now,
        )
        .await?;
        let snapshot = ToolExecutionSnapshot {
            tenant_id: context.tenant_id,
            tool_execution_id: command.tool_execution_id,
            run_id: command.expected_run.run_id,
            stage_execution_id: execution.stage_execution_id.map(stage_id_from_uuid),
            task_id: command.task_id,
            tool_call_id: execution.tool_call_id,
            tool_name: execution.tool_name,
            status: projection.status,
            attempt_count: command.expected_attempt,
            external_ref: persisted_external_ref,
            recovery_action: projection.recovery_action.clone(),
            retry_at: projection.retry_at.map(UnixMicros::new),
            updated_at: UnixMicros::new(db_now),
        };
        let mut follow_ups = Vec::new();
        if projection.status.requires_reconciliation() {
            follow_ups.push(DurableFollowUp::ReconcileTool {
                execution_id: command.tool_execution_id,
            });
        }
        if let Some(not_before) = projection.retry_at {
            follow_ups.push(DurableFollowUp::ScanDueWork {
                not_before: UnixMicros::new(not_before),
            });
        }
        let outcome = encode_tool_receipt(&snapshot)?;
        finish_receipt(
            &transaction,
            context,
            "applied",
            &outcome,
            Some(event_id),
            Some(("tool_execution", execution_id, execution.attempt_count)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;
        Ok(tool_committed(
            CommandDisposition::Applied,
            snapshot,
            Some(command.outcome_event_id),
            follow_ups,
        ))
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
        let run_id = uuid(command.expected_run.run_id.into_bytes());
        let task_id = uuid(command.lease.task_id.into_bytes());
        let run_row = transaction
            .query_opt(
                "SELECT r.status, r.version, r.execution_generation, r.next_event_sequence, \
                        c.sequence, r.workflow_version_id, r.coordinator_agent_version_id, \
                        CASE WHEN r.deadline IS NULL THEN NULL \
                             ELSE (extract(epoch FROM r.deadline) * 1000000)::bigint END \
                 FROM agent_loom.runs r \
                 LEFT JOIN agent_loom.checkpoints c ON c.tenant_id = r.tenant_id \
                    AND c.run_id = r.run_id AND c.checkpoint_id = r.current_checkpoint_id \
                 WHERE r.tenant_id = $1 AND r.run_id = $2 FOR UPDATE OF r",
                &[&tenant_id, &run_id],
            )
            .await
            .map_err(map_database_error)?
            .ok_or_else(|| store_error(StoreErrorCode::NotFound, "run was not found"))?;
        let task_row = transaction
            .query_opt(
                "SELECT run_id, status, generation, attempt, lease_owner, lease_token, \
                        (extract(epoch FROM lease_expires_at) * 1000000)::bigint \
                 FROM agent_loom.tasks \
                 WHERE tenant_id = $1 AND task_id = $2 FOR UPDATE",
                &[&tenant_id, &task_id],
            )
            .await
            .map_err(map_database_error)?
            .ok_or_else(|| store_error(StoreErrorCode::NotFound, "task was not found"))?;
        let locked = LockedCompletion::decode(&run_row, &task_row);
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
                payload_schema_version: 1,
                producer: "worker",
                context,
                correlation_id,
                occurred_at: None,
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

    /// Pauses a non-terminal Run, fences old Workers, preserves planned Tasks,
    /// and persists stop intent for active remote Agent executions.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for stale expectations, idempotency misuse,
    /// missing Runs, or database failures.
    pub async fn pause_run(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: ControlRun,
    ) -> StoreResult<Committed<RunSnapshot>> {
        execute_control(self.config, client, context, command, ControlKind::Pause).await
    }

    /// Resumes a paused Run after unknown external outcomes are cleared and
    /// recomputes status from preserved Tasks and Waits.
    ///
    /// # Errors
    ///
    /// Returns a stable store error if the Run is not paused, recovery is
    /// blocked, expectations are stale, or PostgreSQL rejects the transaction.
    pub async fn resume_run(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: ControlRun,
    ) -> StoreResult<Committed<RunSnapshot>> {
        execute_control(self.config, client, context, command, ControlKind::Resume).await
    }

    /// Atomically makes cancellation terminal, fences Workers, closes open
    /// Tasks/Waits/Stages, and persists external stop/reconciliation intent.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for stale expectations, idempotency misuse,
    /// missing Runs, or database failures.
    pub async fn cancel_run(
        &self,
        client: &mut Client,
        context: &CommandContext,
        command: ControlRun,
    ) -> StoreResult<Committed<RunSnapshot>> {
        execute_control(self.config, client, context, command, ControlKind::Cancel).await
    }
}

#[derive(Debug)]
struct LockedClaimCandidate {
    task_id: Uuid,
    run_id: Uuid,
    stage_execution_id: Option<Uuid>,
    logical_key: String,
    kind: String,
    generation: i64,
    attempt: i64,
    max_attempts: i64,
    available_at: i64,
    next_event_sequence: i64,
}

async fn lock_claim_candidate(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    db_now: i64,
    candidate_window: u32,
) -> StoreResult<Option<LockedClaimCandidate>> {
    let window = i64::from(candidate_window);
    let candidates = transaction
        .query(
            "SELECT t.task_id, t.run_id \
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
             ORDER BY t.priority DESC, t.available_at, t.task_id LIMIT $3",
            &[&tenant_id, &db_now, &window],
        )
        .await
        .map_err(map_database_error)?;

    for candidate in candidates {
        let task_id: Uuid = candidate.get(0);
        let run_id: Uuid = candidate.get(1);
        let Some(run) = transaction
            .query_opt(
                "SELECT execution_generation, next_event_sequence \
                 FROM agent_loom.runs \
                 WHERE tenant_id = $1 AND run_id = $2 AND status IN ('queued', 'running') \
                 FOR UPDATE SKIP LOCKED",
                &[&tenant_id, &run_id],
            )
            .await
            .map_err(map_database_error)?
        else {
            continue;
        };
        let generation: i64 = run.get(0);
        let next_event_sequence: i64 = run.get(1);
        let Some(task) = transaction
            .query_opt(
                "SELECT stage_execution_id, logical_key, kind, generation, attempt, \
                        max_attempts, (extract(epoch FROM available_at) * 1000000)::bigint \
                 FROM agent_loom.tasks \
                 WHERE tenant_id = $1 AND run_id = $2 AND task_id = $3 \
                   AND status IN ('queued', 'retry_scheduled') AND generation = $4 \
                   AND available_at <= \
                       to_timestamp(($5::bigint)::double precision / 1000000.0) \
                   AND (deadline IS NULL OR deadline >= \
                       to_timestamp(($5::bigint)::double precision / 1000000.0)) \
                   AND attempt < max_attempts FOR UPDATE SKIP LOCKED",
                &[&tenant_id, &run_id, &task_id, &generation, &db_now],
            )
            .await
            .map_err(map_database_error)?
        else {
            continue;
        };
        return Ok(Some(LockedClaimCandidate {
            task_id,
            run_id,
            stage_execution_id: task.get(0),
            logical_key: task.get(1),
            kind: task.get(2),
            generation: task.get(3),
            attempt: task.get(4),
            max_attempts: task.get(5),
            available_at: task.get(6),
            next_event_sequence,
        }));
    }
    Ok(None)
}

#[derive(Debug)]
struct LockedWorkerTask {
    run_id: Uuid,
    task_id: Uuid,
    stage_execution_id: Option<Uuid>,
    logical_key: String,
    kind: String,
    task_status: String,
    task_generation: i64,
    attempt: i64,
    max_attempts: i64,
    available_at: i64,
    lease_owner: Option<Uuid>,
    lease_token: Option<Vec<u8>>,
    lease_expires_at: Option<i64>,
    run_status: String,
    run_version: i64,
    execution_generation: i64,
    next_event_sequence: i64,
    workflow_version_id: Option<Uuid>,
    current_checkpoint_id: Option<Uuid>,
    deadline: Option<i64>,
}

impl LockedWorkerTask {
    fn claimed_task(&self, tenant_id: TenantId, lease_expires_at: i64) -> StoreResult<ClaimedTask> {
        Ok(ClaimedTask {
            task: TaskSnapshot {
                tenant_id,
                task_id: task_id_from_uuid(self.task_id),
                run_id: run_id_from_uuid(self.run_id),
                stage_execution_id: self.stage_execution_id.map(stage_id_from_uuid),
                logical_key: LogicalKey::parse(self.logical_key.clone())
                    .map_err(|_| inconsistent("database contains an invalid task logical key"))?,
                kind: parse_task_kind(&self.kind)?,
                status: TaskStatus::Leased,
                generation: nonnegative_u64(self.task_generation, "task generation")?,
                attempt: positive_u32(self.attempt, "task attempt")?,
                max_attempts: positive_u32(self.max_attempts, "task max attempts")?,
                available_at: UnixMicros::new(self.available_at),
            },
            lease_expires_at: UnixMicros::new(lease_expires_at),
        })
    }
}

async fn lock_worker_task(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    task_id: Uuid,
) -> StoreResult<LockedWorkerTask> {
    let run = transaction
        .query_opt(
            "SELECT status, version, execution_generation, next_event_sequence, \
                    workflow_version_id, current_checkpoint_id, \
                    CASE WHEN deadline IS NULL THEN NULL \
                         ELSE (extract(epoch FROM deadline) * 1000000)::bigint END \
             FROM agent_loom.runs \
             WHERE tenant_id = $1 AND run_id = $2 FOR UPDATE",
            &[&tenant_id, &run_id],
        )
        .await
        .map_err(map_database_error)?
        .ok_or_else(|| store_error(StoreErrorCode::NotFound, "run was not found"))?;
    let task = transaction
        .query_opt(
            "SELECT run_id, task_id, stage_execution_id, logical_key, kind, status, \
                    generation, attempt, max_attempts, \
                    (extract(epoch FROM available_at) * 1000000)::bigint, \
                    lease_owner, lease_token, \
                    CASE WHEN lease_expires_at IS NULL THEN NULL \
                         ELSE (extract(epoch FROM lease_expires_at) * 1000000)::bigint END \
             FROM agent_loom.tasks \
             WHERE tenant_id = $1 AND task_id = $2 FOR UPDATE",
            &[&tenant_id, &task_id],
        )
        .await
        .map_err(map_database_error)?
        .ok_or_else(|| store_error(StoreErrorCode::NotFound, "task was not found"))?;
    Ok(LockedWorkerTask {
        run_id: task.get(0),
        task_id: task.get(1),
        stage_execution_id: task.get(2),
        logical_key: task.get(3),
        kind: task.get(4),
        task_status: task.get(5),
        task_generation: task.get(6),
        attempt: task.get(7),
        max_attempts: task.get(8),
        available_at: task.get(9),
        lease_owner: task.get(10),
        lease_token: task.get(11),
        lease_expires_at: task.get(12),
        run_status: run.get(0),
        run_version: run.get(1),
        execution_generation: run.get(2),
        next_event_sequence: run.get(3),
        workflow_version_id: run.get(4),
        current_checkpoint_id: run.get(5),
        deadline: run.get(6),
    })
}

fn validate_worker_fences(
    expected_run: &ExpectedRun,
    lease: &LeaseProof,
    locked: &LockedWorkerTask,
    db_now: i64,
    operation: &str,
) -> StoreResult<()> {
    if locked.run_id != uuid(expected_run.run_id.into_bytes()) {
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
            &format!("paused run rejects task {operation}"),
        ));
    }
    if matches!(
        locked.run_status.as_str(),
        "completed" | "failed" | "cancelled" | "timed_out"
    ) {
        return Err(store_error(StoreErrorCode::TerminalRun, "run is terminal"));
    }
    if expected_run
        .version
        .is_some_and(|expected| i64::try_from(expected).ok() != Some(locked.run_version))
    {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "run version changed",
        ));
    }
    if expected_run
        .execution_generation
        .is_some_and(|expected| i64::try_from(expected).ok() != Some(locked.execution_generation))
    {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "run execution generation changed",
        ));
    }
    let lease_generation = to_i64(lease.execution_generation, "lease generation")?;
    if lease_generation != locked.execution_generation || lease_generation != locked.task_generation
    {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "execution generation changed",
        ));
    }
    if locked.lease_owner != Some(uuid(lease.worker_id.into_bytes()))
        || locked.lease_token.as_deref() != Some(lease.token.as_bytes().as_slice())
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
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FailureTransition {
    task_status: &'static str,
    run_status: &'static str,
    event_type: &'static str,
    retry_at: Option<i64>,
    terminal: bool,
}

impl FailureTransition {
    const fn classify(attempt: i64, max_attempts: i64, retry_at: Option<i64>) -> Self {
        if retry_at.is_some() && attempt < max_attempts {
            Self {
                task_status: "retry_scheduled",
                run_status: "retrying",
                event_type: "task.retry_scheduled",
                retry_at,
                terminal: false,
            }
        } else if attempt >= max_attempts {
            Self {
                task_status: "dead_lettered",
                run_status: "waiting",
                event_type: "task.dead_lettered",
                retry_at: None,
                terminal: false,
            }
        } else {
            Self {
                task_status: "failed",
                run_status: "failed",
                event_type: "task.failed",
                retry_at: None,
                terminal: true,
            }
        }
    }
}

async fn finalize_failed_task(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    locked: &LockedWorkerTask,
    db_now: i64,
    error_code: &str,
    error_json: &Value,
    transition: &FailureTransition,
) -> StoreResult<()> {
    let task_terminal = transition.task_status != "retry_scheduled";
    let updated = transaction
        .execute(
            "UPDATE agent_loom.tasks SET status = $4, \
                available_at = CASE WHEN $5::bigint IS NULL THEN available_at ELSE \
                    to_timestamp(($5::bigint)::double precision / 1000000.0) END, \
                lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, \
                error_code = $6, error_json = $7, \
                completed_at = CASE WHEN $8 THEN \
                    to_timestamp(($9::bigint)::double precision / 1000000.0) ELSE NULL END, \
                updated_at = to_timestamp(($9::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND task_id = $2 AND attempt = $3 AND status = 'leased'",
            &[
                &tenant_id,
                &locked.task_id,
                &locked.attempt,
                &transition.task_status,
                &transition.retry_at,
                &error_code,
                &error_json,
                &task_terminal,
                &db_now,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if updated != 1 {
        return Err(store_error(
            StoreErrorCode::LeaseLost,
            "task lease changed while recording failure",
        ));
    }
    let attempt_updated = transaction
        .execute(
            "UPDATE agent_loom.task_attempts SET \
                finished_at = to_timestamp(($5::bigint)::double precision / 1000000.0), \
                outcome = 'failed', error_code = $4 \
             WHERE tenant_id = $1 AND task_id = $2 AND attempt = $3 \
               AND finished_at IS NULL",
            &[
                &tenant_id,
                &locked.task_id,
                &locked.attempt,
                &error_code,
                &db_now,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if attempt_updated != 1 {
        return Err(inconsistent("leased task has no open matching attempt"));
    }
    Ok(())
}

async fn close_work_after_fatal_failure(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    failed_task_id: Uuid,
    db_now: i64,
) -> StoreResult<()> {
    transaction
        .execute(
            "UPDATE agent_loom.tasks SET status = 'cancelled', \
                lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, \
                error_code = 'run_failed', \
                completed_at = to_timestamp(($4::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($4::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND task_id <> $3 \
               AND status IN ('scheduled', 'queued', 'leased', 'retry_scheduled')",
            &[&tenant_id, &run_id, &failed_task_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    transaction
        .execute(
            "UPDATE agent_loom.task_attempts SET \
                finished_at = to_timestamp(($4::bigint)::double precision / 1000000.0), \
                outcome = 'cancelled', error_code = 'run_failed' \
             WHERE tenant_id = $1 AND run_id = $2 AND task_id <> $3 \
               AND finished_at IS NULL",
            &[&tenant_id, &run_id, &failed_task_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    transaction
        .execute(
            "UPDATE agent_loom.wait_subscriptions SET status = 'cancelled', active_slot = NULL, \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND status = 'open'",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    transaction
        .execute(
            "UPDATE agent_loom.agent_executions SET status = 'cancelled', version = version + 1, \
                error_code = 'run_failed', \
                completed_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND status = 'planned'",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    transaction
        .execute(
            "UPDATE agent_loom.tool_executions SET status = 'failed', \
                error_code = 'run_failed', \
                completed_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 \
               AND status IN ('planned', 'retry_scheduled')",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct WaitCandidate {
    wait_id: Uuid,
    stage_execution_id: Option<Uuid>,
}

#[derive(Debug)]
struct LockedEventRun {
    status: String,
    suspended_from_status: Option<String>,
    version: i64,
    execution_generation: i64,
    next_event_sequence: i64,
    workflow_version_id: Option<Uuid>,
    current_checkpoint_id: Option<Uuid>,
    checkpoint_sequence: Option<i64>,
    deadline: Option<i64>,
}

#[derive(Debug)]
struct LockedWait {
    wait_id: Uuid,
    stage_execution_id: Option<Uuid>,
    wait_type: String,
    expected_event_type: String,
    match_key_hash: Vec<u8>,
    match_contract: Value,
    status: String,
    expires_at: Option<i64>,
    resume_task_id: TaskId,
    resume_logical_key: String,
    resume_task_kind: String,
    resume_priority: i32,
    resume_max_attempts: i64,
    resume_input: Value,
    resume_deadline: Option<i64>,
}

fn validate_apply_event(command: &ApplyEvent) -> StoreResult<()> {
    if command.event_type.is_empty() || command.payload_schema_version == 0 {
        return Err(invalid_command(
            "external event type and payload schema version must be present",
        ));
    }
    if command.signature_verification == SignatureVerification::Failed {
        return Err(invalid_command(
            "external event signature verification failed",
        ));
    }
    Ok(())
}

async fn find_wait_candidate(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    event_type: &str,
    match_key_hash: &[u8],
) -> StoreResult<Option<WaitCandidate>> {
    let rows = transaction
        .query(
            "SELECT wait_id, stage_execution_id \
             FROM agent_loom.wait_subscriptions \
             WHERE tenant_id = $1 AND run_id = $2 AND status = 'open' \
               AND expected_event_type = $3 AND match_key_hash = $4 \
             ORDER BY wait_id LIMIT 2",
            &[&tenant_id, &run_id, &event_type, &match_key_hash],
        )
        .await
        .map_err(map_database_error)?;
    if rows.len() > 1 {
        return Err(store_error(
            StoreErrorCode::WaitMismatch,
            "external event matches more than one open wait",
        ));
    }
    Ok(rows.first().map(|row| WaitCandidate {
        wait_id: row.get(0),
        stage_execution_id: row.get(1),
    }))
}

async fn lock_event_run(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
) -> StoreResult<LockedEventRun> {
    let row = transaction
        .query_opt(
            "SELECT r.status, r.suspended_from_status, r.version, r.execution_generation, \
                    r.next_event_sequence, r.workflow_version_id, r.current_checkpoint_id, \
                    c.sequence, CASE WHEN r.deadline IS NULL THEN NULL \
                        ELSE (extract(epoch FROM r.deadline) * 1000000)::bigint END \
             FROM agent_loom.runs r \
             LEFT JOIN agent_loom.checkpoints c ON c.tenant_id = r.tenant_id \
                AND c.run_id = r.run_id AND c.checkpoint_id = r.current_checkpoint_id \
             WHERE r.tenant_id = $1 AND r.run_id = $2 FOR UPDATE OF r",
            &[&tenant_id, &run_id],
        )
        .await
        .map_err(map_database_error)?
        .ok_or_else(|| store_error(StoreErrorCode::NotFound, "run was not found"))?;
    Ok(LockedEventRun {
        status: row.get(0),
        suspended_from_status: row.get(1),
        version: row.get(2),
        execution_generation: row.get(3),
        next_event_sequence: row.get(4),
        workflow_version_id: row.get(5),
        current_checkpoint_id: row.get(6),
        checkpoint_sequence: row.get(7),
        deadline: row.get(8),
    })
}

fn validate_event_run(
    expected: &ExpectedRun,
    locked: &LockedEventRun,
    db_now: i64,
) -> StoreResult<()> {
    if matches!(
        locked.status.as_str(),
        "completed" | "failed" | "cancelled" | "timed_out"
    ) {
        return Err(store_error(StoreErrorCode::TerminalRun, "run is terminal"));
    }
    if expected
        .version
        .is_some_and(|value| i64::try_from(value).ok() != Some(locked.version))
        || expected
            .execution_generation
            .is_some_and(|value| i64::try_from(value).ok() != Some(locked.execution_generation))
    {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "run expectation changed",
        ));
    }
    if locked.deadline.is_some_and(|deadline| deadline <= db_now) {
        return Err(store_error(
            StoreErrorCode::DeadlineExceeded,
            "run deadline has elapsed",
        ));
    }
    Ok(())
}

async fn classify_wait_miss(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    event_type: &str,
    match_key_hash: &[u8],
    db_now: i64,
) -> StoreResult<StoreError> {
    let row = transaction
        .query_opt(
            "SELECT status, CASE WHEN expires_at IS NULL THEN NULL \
                    ELSE (extract(epoch FROM expires_at) * 1000000)::bigint END \
             FROM agent_loom.wait_subscriptions \
             WHERE tenant_id = $1 AND run_id = $2 AND expected_event_type = $3 \
               AND match_key_hash = $4 \
             ORDER BY updated_at DESC, wait_id LIMIT 1",
            &[&tenant_id, &run_id, &event_type, &match_key_hash],
        )
        .await
        .map_err(map_database_error)?;
    let Some(row) = row else {
        return Ok(store_error(
            StoreErrorCode::WaitMismatch,
            "external event does not match an open wait",
        ));
    };
    let status: &str = row.get(0);
    let expires_at: Option<i64> = row.get(1);
    Ok(match status {
        "consumed" => store_error(
            StoreErrorCode::WaitAlreadyConsumed,
            "matching wait was already consumed",
        ),
        "expired" => store_error(StoreErrorCode::WaitExpired, "matching wait expired"),
        "open" if expires_at.is_some_and(|expires| expires <= db_now) => {
            store_error(StoreErrorCode::WaitExpired, "matching wait expired")
        }
        "cancelled" => store_error(StoreErrorCode::WaitMismatch, "matching wait was cancelled"),
        _ => inconsistent("open wait candidate scan disagrees with wait history"),
    })
}

async fn lock_event_stage(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    stage_execution_id: Uuid,
) -> StoreResult<()> {
    transaction
        .query_opt(
            "SELECT stage_execution_id FROM agent_loom.stage_executions \
             WHERE tenant_id = $1 AND run_id = $2 AND stage_execution_id = $3 FOR UPDATE",
            &[&tenant_id, &run_id, &stage_execution_id],
        )
        .await
        .map_err(map_database_error)?
        .ok_or_else(|| inconsistent("wait references a missing stage"))?;
    Ok(())
}

async fn lock_wait(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    wait_id: Uuid,
) -> StoreResult<LockedWait> {
    let row = transaction
        .query_opt(
            "SELECT wait_id, stage_execution_id, wait_type, expected_event_type, \
                    match_key_hash, match_contract_json, status, \
                    CASE WHEN expires_at IS NULL THEN NULL \
                        ELSE (extract(epoch FROM expires_at) * 1000000)::bigint END, \
                    resume_task_id, resume_logical_key, resume_task_kind, resume_priority, \
                    resume_max_attempts, resume_input_json, \
                    CASE WHEN resume_deadline IS NULL THEN NULL \
                        ELSE (extract(epoch FROM resume_deadline) * 1000000)::bigint END \
             FROM agent_loom.wait_subscriptions \
             WHERE tenant_id = $1 AND run_id = $2 AND wait_id = $3 FOR UPDATE",
            &[&tenant_id, &run_id, &wait_id],
        )
        .await
        .map_err(map_database_error)?
        .ok_or_else(|| inconsistent("wait candidate disappeared"))?;
    Ok(LockedWait {
        wait_id: row.get(0),
        stage_execution_id: row.get(1),
        wait_type: row.get(2),
        expected_event_type: row.get(3),
        match_key_hash: row.get(4),
        match_contract: row.get(5),
        status: row.get(6),
        expires_at: row.get(7),
        resume_task_id: task_id_from_uuid(row.get(8)),
        resume_logical_key: row.get(9),
        resume_task_kind: row.get(10),
        resume_priority: row.get(11),
        resume_max_attempts: row.get(12),
        resume_input: row.get(13),
        resume_deadline: row.get(14),
    })
}

fn validate_locked_wait(command: &ApplyEvent, wait: &LockedWait, db_now: i64) -> StoreResult<()> {
    if wait.status != "open" {
        return Err(match wait.status.as_str() {
            "consumed" => store_error(
                StoreErrorCode::WaitAlreadyConsumed,
                "matching wait was already consumed",
            ),
            "expired" => store_error(StoreErrorCode::WaitExpired, "matching wait expired"),
            _ => store_error(StoreErrorCode::WaitMismatch, "matching wait is not open"),
        });
    }
    if wait.expected_event_type != command.event_type
        || wait.match_key_hash.as_slice() != command.match_key_hash.as_bytes().as_slice()
    {
        return Err(store_error(
            StoreErrorCode::WaitMismatch,
            "external event no longer matches the locked wait",
        ));
    }
    if wait.expires_at.is_some_and(|expires| expires <= db_now) {
        return Err(store_error(
            StoreErrorCode::WaitExpired,
            "matching wait expired",
        ));
    }
    if wait
        .resume_deadline
        .is_some_and(|deadline| deadline <= db_now)
    {
        return Err(store_error(
            StoreErrorCode::DeadlineExceeded,
            "wait resume task deadline has elapsed",
        ));
    }
    positive_u32(wait.resume_max_attempts, "wait resume max attempts")?;
    LogicalKey::parse(wait.resume_logical_key.clone())
        .map_err(|_| inconsistent("wait contains an invalid resume task logical key"))?;
    parse_task_kind(&wait.resume_task_kind)?;
    validate_match_contract(&wait.match_contract, &json_value(&command.payload)?)?;
    Ok(())
}

fn validate_match_contract(contract: &Value, payload: &Value) -> StoreResult<()> {
    let contract = contract
        .as_object()
        .ok_or_else(|| inconsistent("wait match contract is not a JSON object"))?;
    if let Some(required) = contract.get("required") {
        let required = required
            .as_array()
            .ok_or_else(|| inconsistent("wait required contract is not an array"))?;
        let payload = payload.as_object().ok_or_else(|| {
            store_error(
                StoreErrorCode::WaitMismatch,
                "event payload does not satisfy the wait contract",
            )
        })?;
        for field in required {
            let field = field
                .as_str()
                .ok_or_else(|| inconsistent("wait required contract contains a non-string"))?;
            if !payload.contains_key(field) {
                return Err(store_error(
                    StoreErrorCode::WaitMismatch,
                    "event payload does not satisfy the wait contract",
                ));
            }
        }
    }
    if let Some(equals) = contract.get("equals") {
        let equals = equals
            .as_object()
            .ok_or_else(|| inconsistent("wait equals contract is not an object"))?;
        let payload = payload.as_object().ok_or_else(|| {
            store_error(
                StoreErrorCode::WaitMismatch,
                "event payload does not satisfy the wait contract",
            )
        })?;
        if equals
            .iter()
            .any(|(key, expected)| payload.get(key) != Some(expected))
        {
            return Err(store_error(
                StoreErrorCode::WaitMismatch,
                "event payload does not satisfy the wait contract",
            ));
        }
    }
    Ok(())
}

async fn consume_wait(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    wait_id: Uuid,
    event_id: Uuid,
    db_now: i64,
) -> StoreResult<()> {
    let updated = transaction
        .execute(
            "UPDATE agent_loom.wait_subscriptions SET status = 'consumed', active_slot = NULL, \
                consumed_by_event_id = $3, \
                consumed_at = to_timestamp(($4::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($4::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND wait_id = $2 AND status = 'open'",
            &[&tenant_id, &wait_id, &event_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    if updated != 1 {
        return Err(store_error(
            StoreErrorCode::WaitAlreadyConsumed,
            "matching wait was consumed concurrently",
        ));
    }
    Ok(())
}

async fn reactivate_waiting_stage(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    stage_execution_id: Option<Uuid>,
    wait_type: &str,
    db_now: i64,
) -> StoreResult<()> {
    let Some(stage_execution_id) = stage_execution_id else {
        return Ok(());
    };
    if wait_type != "approval" {
        return Ok(());
    }
    let updated = transaction
        .execute(
            "UPDATE agent_loom.stage_executions SET status = 'active', version = version + 1, \
                updated_at = to_timestamp(($4::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND stage_execution_id = $3 \
               AND status = 'waiting_approval'",
            &[&tenant_id, &run_id, &stage_execution_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    if updated != 1 {
        return Err(store_error(
            StoreErrorCode::InvalidTransition,
            "approval wait stage is not waiting for approval",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_wait_resume_task(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    event_id: Uuid,
    db_now: i64,
    generation: i64,
    checkpoint_sequence: i64,
    wait: &LockedWait,
    command: &ApplyEvent,
    paused: bool,
) -> StoreResult<()> {
    let task_id = uuid(wait.resume_task_id.into_bytes());
    let status = if paused { "scheduled" } else { "queued" };
    let max_attempts = wait.resume_max_attempts;
    let input = json!({
        "wait_id": wait.wait_id,
        "event_id": uuid(command.event_id.into_bytes()),
        "event_type": &command.event_type,
        "event_payload": json_value(&command.payload)?,
        "resume_input": &wait.resume_input,
    });
    let inserted = transaction
        .execute(
            "INSERT INTO agent_loom.tasks (\
                task_id, tenant_id, run_id, stage_execution_id, logical_key, kind, status, \
                generation, based_on_checkpoint_sequence, priority, available_at, attempt, \
                max_attempts, lease_owner, lease_token, lease_expires_at, input_json, \
                result_json, error_code, error_json, deadline, created_event_id, created_at, \
                updated_at, completed_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
                to_timestamp(($11::bigint)::double precision / 1000000.0), 0, $12, \
                NULL, NULL, NULL, $13, NULL, NULL, NULL, \
                to_timestamp(($14::bigint)::double precision / 1000000.0), $15, \
                to_timestamp(($11::bigint)::double precision / 1000000.0), \
                to_timestamp(($11::bigint)::double precision / 1000000.0), NULL)",
            &[
                &task_id,
                &tenant_id,
                &run_id,
                &wait.stage_execution_id,
                &wait.resume_logical_key,
                &wait.resume_task_kind,
                &status,
                &generation,
                &checkpoint_sequence,
                &wait.resume_priority,
                &db_now,
                &max_attempts,
                &input,
                &wait.resume_deadline,
                &event_id,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if inserted != 1 {
        return Err(inconsistent("resume task insert did not affect one row"));
    }
    Ok(())
}

async fn run_has_leased_task(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
) -> StoreResult<bool> {
    transaction
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM agent_loom.tasks \
             WHERE tenant_id = $1 AND run_id = $2 AND status = 'leased')",
            &[&tenant_id, &run_id],
        )
        .await
        .map(|row| row.get(0))
        .map_err(map_database_error)
}

#[derive(Debug)]
struct LockedToolExecution {
    tool_execution_id: Uuid,
    run_id: Uuid,
    stage_execution_id: Option<Uuid>,
    task_id: Uuid,
    tool_call_id: String,
    tool_name: String,
    request_hash: Vec<u8>,
    status: String,
    attempt_count: i64,
    external_ref: Option<String>,
    recovery_action: Option<String>,
    retry_at: Option<i64>,
    updated_at: i64,
}

impl LockedToolExecution {
    fn snapshot(&self, tenant_id: TenantId) -> StoreResult<ToolExecutionSnapshot> {
        Ok(ToolExecutionSnapshot {
            tenant_id,
            tool_execution_id: ToolExecutionId::from_bytes(self.tool_execution_id.into_bytes()),
            run_id: run_id_from_uuid(self.run_id),
            stage_execution_id: self.stage_execution_id.map(stage_id_from_uuid),
            task_id: task_id_from_uuid(self.task_id),
            tool_call_id: self.tool_call_id.clone(),
            tool_name: self.tool_name.clone(),
            status: parse_tool_status(&self.status)?,
            attempt_count: positive_u32(self.attempt_count, "tool attempt count")?,
            external_ref: self.external_ref.clone(),
            recovery_action: self.recovery_action.clone(),
            retry_at: self.retry_at.map(UnixMicros::new),
            updated_at: UnixMicros::new(self.updated_at),
        })
    }
}

fn validate_prepare_tool(command: &PrepareToolExecution) -> StoreResult<()> {
    if command.tool_call_id.is_empty() || command.tool_name.is_empty() {
        return Err(invalid_command(
            "tool call identity and tool name must not be empty",
        ));
    }
    Ok(())
}

async fn lock_tool_by_idempotency(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    scope: &str,
    key: &str,
) -> StoreResult<Option<LockedToolExecution>> {
    transaction
        .query_opt(
            "SELECT tool_execution_id, run_id, stage_execution_id, task_id, tool_call_id, \
                    tool_name, request_hash, status, attempt_count, external_ref, \
                    recovery_action, CASE WHEN retry_at IS NULL THEN NULL \
                        ELSE (extract(epoch FROM retry_at) * 1000000)::bigint END, \
                    (extract(epoch FROM updated_at) * 1000000)::bigint \
             FROM agent_loom.tool_executions \
             WHERE tenant_id = $1 AND idempotency_scope = $2 AND idempotency_key = $3 \
             FOR UPDATE",
            &[&tenant_id, &scope, &key],
        )
        .await
        .map(|row| {
            row.map(|row| LockedToolExecution {
                tool_execution_id: row.get(0),
                run_id: row.get(1),
                stage_execution_id: row.get(2),
                task_id: row.get(3),
                tool_call_id: row.get(4),
                tool_name: row.get(5),
                request_hash: row.get(6),
                status: row.get(7),
                attempt_count: row.get(8),
                external_ref: row.get(9),
                recovery_action: row.get(10),
                retry_at: row.get(11),
                updated_at: row.get(12),
            })
        })
        .map_err(map_database_error)
}

async fn lock_tool_by_id(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    execution_id: Uuid,
) -> StoreResult<LockedToolExecution> {
    let row = transaction
        .query_opt(
            "SELECT tool_execution_id, run_id, stage_execution_id, task_id, tool_call_id, \
                    tool_name, request_hash, status, attempt_count, external_ref, \
                    recovery_action, CASE WHEN retry_at IS NULL THEN NULL \
                        ELSE (extract(epoch FROM retry_at) * 1000000)::bigint END, \
                    (extract(epoch FROM updated_at) * 1000000)::bigint \
             FROM agent_loom.tool_executions \
             WHERE tenant_id = $1 AND tool_execution_id = $2 FOR UPDATE",
            &[&tenant_id, &execution_id],
        )
        .await
        .map_err(map_database_error)?
        .ok_or_else(|| store_error(StoreErrorCode::NotFound, "tool execution was not found"))?;
    Ok(LockedToolExecution {
        tool_execution_id: row.get(0),
        run_id: row.get(1),
        stage_execution_id: row.get(2),
        task_id: row.get(3),
        tool_call_id: row.get(4),
        tool_name: row.get(5),
        request_hash: row.get(6),
        status: row.get(7),
        attempt_count: row.get(8),
        external_ref: row.get(9),
        recovery_action: row.get(10),
        retry_at: row.get(11),
        updated_at: row.get(12),
    })
}

#[derive(Debug)]
struct ProjectedToolOutcome {
    status: ToolExecutionStatus,
    result: Option<Value>,
    error_code: Option<String>,
    retry_class: Option<&'static str>,
    recovery_action: Option<String>,
    external_ref: Option<String>,
    retry_at: Option<i64>,
    attempt_outcome: &'static str,
    event_type: &'static str,
}

#[allow(clippy::too_many_lines)]
fn project_tool_outcome(outcome: &ToolRecordedOutcome) -> StoreResult<ProjectedToolOutcome> {
    let projected = match outcome {
        ToolRecordedOutcome::Completed { result } => ProjectedToolOutcome {
            status: ToolExecutionStatus::Succeeded,
            result: Some(json_value(result)?),
            error_code: None,
            retry_class: None,
            recovery_action: None,
            external_ref: None,
            retry_at: None,
            attempt_outcome: "completed",
            event_type: "tool.execution_succeeded",
        },
        ToolRecordedOutcome::Accepted { external_ref } => {
            if external_ref.is_empty() {
                return Err(invalid_command("accepted Tool outcome has no external ref"));
            }
            ProjectedToolOutcome {
                status: ToolExecutionStatus::Executing,
                result: None,
                error_code: None,
                retry_class: None,
                recovery_action: None,
                external_ref: Some(external_ref.clone()),
                retry_at: None,
                attempt_outcome: "accepted",
                event_type: "tool.execution_accepted",
            }
        }
        ToolRecordedOutcome::Failed {
            error_code,
            retry,
            retry_at,
        } => {
            if error_code.is_empty() {
                return Err(invalid_command("failed Tool outcome has no error code"));
            }
            let (status, retry_class, recovery_action) = match retry {
                ExecutionRetryClass::Never => (ToolExecutionStatus::Failed, "never", None),
                ExecutionRetryClass::SameRequestBackoff => (
                    ToolExecutionStatus::RetryScheduled,
                    "same_request_backoff",
                    Some("retry_same_request".to_owned()),
                ),
                ExecutionRetryClass::ReconnectAndResume => (
                    ToolExecutionStatus::Reconciling,
                    "reconnect_and_resume",
                    Some("reconnect_and_resume".to_owned()),
                ),
                ExecutionRetryClass::QueryOutcome => (
                    ToolExecutionStatus::Reconciling,
                    "query_outcome",
                    Some("query_outcome".to_owned()),
                ),
                ExecutionRetryClass::ManualReview => (
                    ToolExecutionStatus::ManualReview,
                    "manual_review",
                    Some("manual_review".to_owned()),
                ),
            };
            let retry_at = retry_at.map(UnixMicros::get);
            if (status == ToolExecutionStatus::RetryScheduled) != retry_at.is_some() {
                return Err(invalid_command(
                    "Tool retry time must be present only for scheduled backoff",
                ));
            }
            ProjectedToolOutcome {
                status,
                result: None,
                error_code: Some(error_code.clone()),
                retry_class: Some(retry_class),
                recovery_action,
                external_ref: None,
                retry_at,
                attempt_outcome: "failed",
                event_type: "tool.execution_failed",
            }
        }
        ToolRecordedOutcome::Uncertain {
            external_ref,
            recovery_action,
        } => {
            if external_ref.as_ref().is_some_and(String::is_empty) || recovery_action.is_empty() {
                return Err(invalid_command(
                    "uncertain Tool outcome has invalid recovery metadata",
                ));
            }
            ProjectedToolOutcome {
                status: ToolExecutionStatus::OutcomeUnknown,
                result: None,
                error_code: None,
                retry_class: Some("query_outcome"),
                recovery_action: Some(recovery_action.clone()),
                external_ref: external_ref.clone(),
                retry_at: None,
                attempt_outcome: "uncertain",
                event_type: "tool.execution_outcome_unknown",
            }
        }
        ToolRecordedOutcome::Compensated { result } => ProjectedToolOutcome {
            status: ToolExecutionStatus::Compensated,
            result: Some(json_value(result)?),
            error_code: None,
            retry_class: None,
            recovery_action: None,
            external_ref: None,
            retry_at: None,
            attempt_outcome: "completed",
            event_type: "tool.execution_compensated",
        },
    };
    Ok(projected)
}

fn validate_existing_tool(
    command: &PrepareToolExecution,
    task: &LockedWorkerTask,
    existing: &LockedToolExecution,
) -> StoreResult<()> {
    if existing.run_id != task.run_id
        || existing.task_id != task.task_id
        || existing.stage_execution_id != task.stage_execution_id
        || existing.tool_call_id != command.tool_call_id
        || existing.tool_name != command.tool_name
        || existing.request_hash.as_slice() != command.request_hash.as_bytes().as_slice()
    {
        return Err(store_error(
            StoreErrorCode::IdempotencyKeyReused,
            "tool idempotency key was reused for a different request",
        ));
    }
    Ok(())
}

async fn advance_run_event_cursor(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    version: i64,
    generation: i64,
    db_now: i64,
) -> StoreResult<()> {
    let updated = transaction
        .execute(
            "UPDATE agent_loom.runs SET version = version + 1, \
                next_event_sequence = next_event_sequence + 1, \
                updated_at = to_timestamp(($5::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND version = $3 \
               AND execution_generation = $4",
            &[&tenant_id, &run_id, &version, &generation, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    if updated != 1 {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "run changed while appending an execution event",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ControlKind {
    Pause,
    Resume,
    Cancel,
}

impl ControlKind {
    const fn event_type(self) -> &'static str {
        match self {
            Self::Pause => "run.paused",
            Self::Resume => "run.resumed",
            Self::Cancel => "run.cancelled",
        }
    }

    fn is_no_op(self, status: RunStatus) -> bool {
        match self {
            Self::Pause => status == RunStatus::Paused || status.is_terminal(),
            Self::Cancel => status.is_terminal(),
            Self::Resume => false,
        }
    }
}

#[derive(Debug)]
struct LockedControlRun {
    snapshot: RunSnapshot,
    version_i64: i64,
    generation_i64: i64,
    next_event_sequence_i64: i64,
}

impl LockedControlRun {
    fn decode(tenant_id: TenantId, run_id: RunId, row: &Row) -> StoreResult<Self> {
        let version_i64 = row.get(3);
        let generation_i64 = row.get(4);
        let next_event_sequence_i64 = row.get(5);
        let suspended_from_status = row
            .get::<_, Option<&str>>(2)
            .map(parse_run_status)
            .transpose()?;
        Ok(Self {
            snapshot: RunSnapshot {
                tenant_id,
                run_id,
                workflow_version_id: row.get::<_, Option<Uuid>>(0).map(workflow_id_from_uuid),
                status: parse_run_status(row.get(1))?,
                suspended_from_status,
                version: nonnegative_u64(version_i64, "run version")?,
                execution_generation: nonnegative_u64(generation_i64, "execution generation")?,
                next_event_sequence: nonnegative_u64(next_event_sequence_i64, "event sequence")?,
                current_checkpoint_id: row
                    .get::<_, Option<Uuid>>(6)
                    .map(|value| CheckpointId::from_bytes(value.into_bytes())),
                terminal_event_id: row.get::<_, Option<Uuid>>(7).map(event_id_from_uuid),
                deadline: row.get::<_, Option<i64>>(8).map(UnixMicros::new),
                updated_at: UnixMicros::new(row.get(9)),
            },
            version_i64,
            generation_i64,
            next_event_sequence_i64,
        })
    }
}

#[allow(clippy::too_many_lines)]
async fn execute_control(
    config: PostgresTransactionConfig,
    client: &mut Client,
    context: &CommandContext,
    command: ControlRun,
    kind: ControlKind,
) -> StoreResult<Committed<RunSnapshot>> {
    if command.reason.trim().is_empty() {
        return Err(invalid_command("control reason must not be empty"));
    }
    let transaction = client.transaction().await.map_err(map_database_error)?;
    let db_now = database_now(&transaction).await?;
    match acquire_receipt(&transaction, context, db_now, config.receipt_ttl_micros).await? {
        ReceiptGuard::Existing(receipt) => {
            let snapshot = decode_run_receipt(context.tenant_id, &receipt.outcome)?;
            transaction.commit().await.map_err(map_database_error)?;
            return Ok(control_committed(
                CommandDisposition::Duplicate,
                snapshot,
                receipt.event_id.map(event_id_from_uuid),
                Vec::new(),
            ));
        }
        ReceiptGuard::Acquired => {}
    }

    let tenant_id = uuid(context.tenant_id.into_bytes());
    let run_id = uuid(command.expected_run.run_id.into_bytes());
    let row = transaction
        .query_opt(
            "SELECT workflow_version_id, status, suspended_from_status, version, \
                    execution_generation, next_event_sequence, current_checkpoint_id, \
                    terminal_event_id, \
                    CASE WHEN deadline IS NULL THEN NULL \
                         ELSE (extract(epoch FROM deadline) * 1000000)::bigint END, \
                    (extract(epoch FROM updated_at) * 1000000)::bigint \
             FROM agent_loom.runs \
             WHERE tenant_id = $1 AND run_id = $2 FOR UPDATE",
            &[&tenant_id, &run_id],
        )
        .await
        .map_err(map_database_error)?
        .ok_or_else(|| store_error(StoreErrorCode::NotFound, "run was not found"))?;
    let locked = LockedControlRun::decode(context.tenant_id, command.expected_run.run_id, &row)?;

    if kind.is_no_op(locked.snapshot.status) {
        let outcome = encode_run_receipt(&locked.snapshot)?;
        finish_receipt(
            &transaction,
            context,
            "no_op",
            &outcome,
            None,
            Some(("run", run_id, locked.version_i64)),
        )
        .await?;
        transaction.commit().await.map_err(map_database_error)?;
        return Ok(control_committed(
            CommandDisposition::NoOp,
            locked.snapshot,
            None,
            Vec::new(),
        ));
    }
    validate_control_expectations(&command, &locked)?;
    if kind == ControlKind::Resume && locked.snapshot.status != RunStatus::Paused {
        return Err(store_error(
            StoreErrorCode::InvalidTransition,
            "only a paused run can be resumed",
        ));
    }
    if kind == ControlKind::Resume
        && locked
            .snapshot
            .deadline
            .is_some_and(|deadline| deadline.get() <= db_now)
    {
        return Err(store_error(
            StoreErrorCode::DeadlineExceeded,
            "run deadline has expired",
        ));
    }

    let event_id = uuid(command.event_id.into_bytes());
    let correlation_id = uuid(context.correlation_id.into_bytes());
    let event_payload = json!({
        "reason": &command.reason,
        "previous_status": run_status(locked.snapshot.status),
        "previous_version": locked.snapshot.version,
        "previous_generation": locked.snapshot.execution_generation,
    });
    let transition = match kind {
        ControlKind::Pause => {
            freeze_leased_tasks(&transaction, tenant_id, run_id, db_now).await?;
            finalize_open_task_attempts(&transaction, tenant_id, run_id, db_now).await?;
            let (blocked, follow_ups) =
                request_external_stops(&transaction, tenant_id, run_id, db_now).await?;
            ControlTransition {
                status: RunStatus::Paused,
                suspended_from_status: Some(locked.snapshot.status),
                generation: locked
                    .snapshot
                    .execution_generation
                    .checked_add(1)
                    .ok_or_else(|| inconsistent("run generation overflow"))?,
                terminal: false,
                follow_ups,
                blocked_reason: blocked.then_some("external_execution_recovery_required"),
            }
        }
        ControlKind::Resume => {
            ensure_resume_is_safe(&transaction, tenant_id, run_id).await?;
            rebase_preserved_tasks(
                &transaction,
                tenant_id,
                run_id,
                locked.generation_i64,
                db_now,
            )
            .await?;
            ControlTransition {
                status: project_resumed_status(&transaction, tenant_id, run_id, db_now).await?,
                suspended_from_status: None,
                generation: locked.snapshot.execution_generation,
                terminal: false,
                follow_ups: Vec::new(),
                blocked_reason: None,
            }
        }
        ControlKind::Cancel => {
            cancel_run_children(&transaction, tenant_id, run_id, db_now).await?;
            finalize_open_task_attempts(&transaction, tenant_id, run_id, db_now).await?;
            let (_, mut follow_ups) =
                request_external_stops(&transaction, tenant_id, run_id, db_now).await?;
            follow_ups.extend(
                mark_executing_tools_uncertain(&transaction, tenant_id, run_id, db_now).await?,
            );
            ControlTransition {
                status: RunStatus::Cancelled,
                suspended_from_status: None,
                generation: locked
                    .snapshot
                    .execution_generation
                    .checked_add(1)
                    .ok_or_else(|| inconsistent("run generation overflow"))?,
                terminal: true,
                follow_ups,
                blocked_reason: None,
            }
        }
    };

    insert_event(
        &transaction,
        EventInsert {
            event_id,
            tenant_id,
            run_id,
            sequence: locked.next_event_sequence_i64,
            event_type: kind.event_type(),
            payload: &event_payload,
            payload_schema_version: 1,
            producer: "control-plane",
            context,
            correlation_id,
            occurred_at: None,
            recorded_at: db_now,
        },
    )
    .await?;
    let next_event_sequence = locked
        .snapshot
        .next_event_sequence
        .checked_add(1)
        .ok_or_else(|| inconsistent("run event sequence overflow"))?;
    let next_event_sequence_i64 = to_i64(next_event_sequence, "event sequence")?;
    let generation_i64 = to_i64(transition.generation, "execution generation")?;
    let status_value = run_status(transition.status);
    let suspended_value = transition.suspended_from_status.map(run_status);
    let next_version_i64 = locked
        .version_i64
        .checked_add(1)
        .ok_or_else(|| inconsistent("run version overflow"))?;
    let updated = transaction
        .execute(
            "UPDATE agent_loom.runs SET status = $5, suspended_from_status = $6, \
                version = $7, execution_generation = $8, next_event_sequence = $9, \
                resume_blocked_reason = $10, \
                terminal_event_id = CASE WHEN $11 THEN $12 ELSE terminal_event_id END, \
                terminal_at = CASE WHEN $11 THEN \
                    to_timestamp(($13::bigint)::double precision / 1000000.0) \
                    ELSE terminal_at END, \
                updated_at = to_timestamp(($13::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND version = $3 \
               AND execution_generation = $4",
            &[
                &tenant_id,
                &run_id,
                &locked.version_i64,
                &locked.generation_i64,
                &status_value,
                &suspended_value,
                &next_version_i64,
                &generation_i64,
                &next_event_sequence_i64,
                &transition.blocked_reason,
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
            "run changed while applying control command",
        ));
    }

    let snapshot = RunSnapshot {
        tenant_id: context.tenant_id,
        run_id: command.expected_run.run_id,
        workflow_version_id: locked.snapshot.workflow_version_id,
        status: transition.status,
        suspended_from_status: transition.suspended_from_status,
        version: nonnegative_u64(next_version_i64, "run version")?,
        execution_generation: transition.generation,
        next_event_sequence,
        current_checkpoint_id: locked.snapshot.current_checkpoint_id,
        terminal_event_id: transition.terminal.then_some(command.event_id),
        deadline: locked.snapshot.deadline,
        updated_at: UnixMicros::new(db_now),
    };
    let outcome = encode_run_receipt(&snapshot)?;
    finish_receipt(
        &transaction,
        context,
        "applied",
        &outcome,
        Some(event_id),
        Some(("run", run_id, next_version_i64)),
    )
    .await?;
    transaction.commit().await.map_err(map_database_error)?;
    Ok(control_committed(
        CommandDisposition::Applied,
        snapshot,
        Some(command.event_id),
        transition.follow_ups,
    ))
}

#[derive(Debug)]
struct ControlTransition {
    status: RunStatus,
    suspended_from_status: Option<RunStatus>,
    generation: u64,
    terminal: bool,
    follow_ups: Vec<DurableFollowUp>,
    blocked_reason: Option<&'static str>,
}

fn validate_control_expectations(
    command: &ControlRun,
    locked: &LockedControlRun,
) -> StoreResult<()> {
    if command
        .expected_run
        .version
        .is_some_and(|expected| expected != locked.snapshot.version)
        || command
            .expected_run
            .execution_generation
            .is_some_and(|expected| expected != locked.snapshot.execution_generation)
    {
        return Err(store_error(
            StoreErrorCode::VersionConflict,
            "run expectation changed",
        ));
    }
    Ok(())
}

async fn finalize_open_task_attempts(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    db_now: i64,
) -> StoreResult<()> {
    transaction
        .execute(
            "UPDATE agent_loom.task_attempts SET \
                finished_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                outcome = 'cancelled', error_code = NULL \
             WHERE tenant_id = $1 AND run_id = $2 AND finished_at IS NULL",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

async fn freeze_leased_tasks(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    db_now: i64,
) -> StoreResult<()> {
    transaction
        .execute(
            "UPDATE agent_loom.tasks SET status = 'retry_scheduled', \
                available_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND status = 'leased'",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

async fn ensure_resume_is_safe(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
) -> StoreResult<()> {
    let row = transaction
        .query_one(
            "SELECT \
                EXISTS (SELECT 1 FROM agent_loom.tool_executions \
                        WHERE tenant_id = $1 AND run_id = $2 \
                          AND status IN ('executing', 'outcome_unknown', 'reconciling', 'manual_review')) \
                OR EXISTS (SELECT 1 FROM agent_loom.agent_executions \
                           WHERE tenant_id = $1 AND run_id = $2 \
                             AND status IN (\
                                'submitting', 'running', 'stopping', 'outcome_unknown', \
                                'reconciling', 'manual_review'))",
            &[&tenant_id, &run_id],
        )
        .await
        .map_err(map_database_error)?;
    if row.get::<_, bool>(0) {
        return Err(store_error(
            StoreErrorCode::PauseRecoveryRequired,
            "external execution recovery blocks resume",
        ));
    }
    Ok(())
}

async fn rebase_preserved_tasks(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    generation: i64,
    db_now: i64,
) -> StoreResult<()> {
    transaction
        .execute(
            "UPDATE agent_loom.tasks SET generation = $3, \
                status = CASE \
                    WHEN status = 'scheduled' AND available_at <= \
                        to_timestamp(($4::bigint)::double precision / 1000000.0) \
                    THEN 'queued' ELSE status END, \
                updated_at = to_timestamp(($4::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 \
               AND status IN ('scheduled', 'queued', 'retry_scheduled') \
               AND (generation <> $3 OR (status = 'scheduled' AND available_at <= \
                    to_timestamp(($4::bigint)::double precision / 1000000.0)))",
            &[&tenant_id, &run_id, &generation, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

async fn project_resumed_status(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    db_now: i64,
) -> StoreResult<RunStatus> {
    let row = transaction
        .query_one(
            "SELECT \
                EXISTS (SELECT 1 FROM agent_loom.tasks \
                        WHERE tenant_id = $1 AND run_id = $2 AND status = 'leased'), \
                EXISTS (SELECT 1 FROM agent_loom.tasks \
                        WHERE tenant_id = $1 AND run_id = $2 \
                          AND status IN ('queued', 'retry_scheduled') \
                          AND available_at <= \
                              to_timestamp(($3::bigint)::double precision / 1000000.0)), \
                EXISTS (SELECT 1 FROM agent_loom.wait_subscriptions \
                        WHERE tenant_id = $1 AND run_id = $2 AND status = 'open' \
                          AND wait_type = 'approval'), \
                EXISTS (SELECT 1 FROM agent_loom.wait_subscriptions \
                        WHERE tenant_id = $1 AND run_id = $2 AND status = 'open' \
                          AND wait_type <> 'approval'), \
                EXISTS (SELECT 1 FROM agent_loom.tasks \
                        WHERE tenant_id = $1 AND run_id = $2 \
                          AND status IN ('scheduled', 'retry_scheduled') \
                          AND available_at > \
                              to_timestamp(($3::bigint)::double precision / 1000000.0))",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    match (
        row.get::<_, bool>(0),
        row.get::<_, bool>(1),
        row.get::<_, bool>(2),
        row.get::<_, bool>(3),
        row.get::<_, bool>(4),
    ) {
        (true, _, _, _, _) => Ok(RunStatus::Running),
        (_, true, _, _, _) => Ok(RunStatus::Queued),
        (_, _, true, _, _) => Ok(RunStatus::ApprovalRequired),
        (_, _, _, true, _) => Ok(RunStatus::Waiting),
        (_, _, _, _, true) => Ok(RunStatus::Retrying),
        _ => Err(store_error(
            StoreErrorCode::PauseRecoveryRequired,
            "paused run has no resumable task or wait",
        )),
    }
}

async fn cancel_run_children(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    db_now: i64,
) -> StoreResult<()> {
    transaction
        .execute(
            "UPDATE agent_loom.stage_executions SET status = 'cancelled', \
                version = version + 1, \
                completed_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 \
               AND status IN ('planned', 'active', 'waiting_approval', 'rework_required')",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    transaction
        .execute(
            "UPDATE agent_loom.tasks SET status = 'cancelled', \
                lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, \
                completed_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 \
               AND status IN ('scheduled', 'queued', 'leased', 'retry_scheduled')",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    transaction
        .execute(
            "UPDATE agent_loom.wait_subscriptions SET status = 'cancelled', active_slot = NULL, \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND status = 'open'",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    transaction
        .execute(
            "UPDATE agent_loom.agent_executions SET status = 'cancelled', version = version + 1, \
                error_code = 'run_cancelled', \
                completed_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND status = 'planned'",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    transaction
        .execute(
            "UPDATE agent_loom.tool_executions SET status = 'failed', \
                error_code = 'run_cancelled', \
                completed_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 \
               AND status IN ('planned', 'retry_scheduled')",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

async fn request_external_stops(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    db_now: i64,
) -> StoreResult<(bool, Vec<DurableFollowUp>)> {
    let rows = transaction
        .query(
            "UPDATE agent_loom.agent_executions SET status = 'stopping', version = version + 1, \
                stop_requested_at = to_timestamp(($3::bigint)::double precision / 1000000.0), \
                updated_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND status IN ('submitting', 'running') \
             RETURNING agent_execution_id",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    let follow_ups = rows
        .into_iter()
        .map(|row| DurableFollowUp::StopAgent {
            execution_id: AgentExecutionId::from_bytes(row.get::<_, Uuid>(0).into_bytes()),
        })
        .collect();
    let blocked = transaction
        .query_one(
            "SELECT \
                EXISTS (SELECT 1 FROM agent_loom.tool_executions \
                        WHERE tenant_id = $1 AND run_id = $2 \
                          AND status IN ('executing', 'outcome_unknown', 'reconciling', 'manual_review')) \
                OR EXISTS (SELECT 1 FROM agent_loom.agent_executions \
                           WHERE tenant_id = $1 AND run_id = $2 \
                             AND status IN (\
                                'submitting', 'running', 'stopping', 'outcome_unknown', \
                                'reconciling', 'manual_review'))",
            &[&tenant_id, &run_id],
        )
        .await
        .map_err(map_database_error)?
        .get(0);
    Ok((blocked, follow_ups))
}

async fn mark_executing_tools_uncertain(
    transaction: &Transaction<'_>,
    tenant_id: Uuid,
    run_id: Uuid,
    db_now: i64,
) -> StoreResult<Vec<DurableFollowUp>> {
    let rows = transaction
        .query(
            "UPDATE agent_loom.tool_executions SET status = 'outcome_unknown', \
                recovery_action = 'query_outcome', updated_at = \
                    to_timestamp(($3::bigint)::double precision / 1000000.0) \
             WHERE tenant_id = $1 AND run_id = $2 AND status = 'executing' \
             RETURNING tool_execution_id",
            &[&tenant_id, &run_id, &db_now],
        )
        .await
        .map_err(map_database_error)?;
    Ok(rows
        .into_iter()
        .map(|row| DurableFollowUp::ReconcileTool {
            execution_id: ToolExecutionId::from_bytes(row.get::<_, Uuid>(0).into_bytes()),
        })
        .collect())
}

fn control_committed(
    disposition: CommandDisposition,
    snapshot: RunSnapshot,
    event_id: Option<EventId>,
    durable_follow_ups: Vec<DurableFollowUp>,
) -> Committed<RunSnapshot> {
    let run_id = snapshot.run_id;
    let mut post_commit_hints = vec![PostCommitHint::InvalidateRunCache { run_id }];
    if event_id.is_some() {
        post_commit_hints.push(PostCommitHint::RunEventsAvailable { run_id });
    }
    if snapshot.status != RunStatus::Paused && !snapshot.status.is_terminal() {
        post_commit_hints.push(PostCommitHint::WakeWorkers);
    }
    Committed {
        disposition,
        value: snapshot,
        event_ids: event_id.into_iter().collect(),
        durable_follow_ups,
        post_commit_hints,
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
    payload_schema_version: i64,
    producer: &'a str,
    context: &'a CommandContext,
    correlation_id: Uuid,
    occurred_at: Option<i64>,
    recorded_at: i64,
}

async fn insert_event(transaction: &Transaction<'_>, event: EventInsert<'_>) -> StoreResult<()> {
    transaction
        .execute(
            "INSERT INTO agent_loom.events (\
                event_id, tenant_id, run_id, sequence, event_type, payload_json, \
                payload_schema_version, producer, actor_ref, correlation_id, causation_id, \
                idempotency_key, occurred_at, recorded_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, \
                to_timestamp(($12::bigint)::double precision / 1000000.0), \
                to_timestamp(($13::bigint)::double precision / 1000000.0))",
            &[
                &event.event_id,
                &event.tenant_id,
                &event.run_id,
                &event.sequence,
                &event.event_type,
                &event.payload,
                &event.payload_schema_version,
                &event.producer,
                &event.context.actor_ref,
                &event.correlation_id,
                &event.context.idempotency_key.as_str(),
                &event.occurred_at,
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
    fn decode(run: &Row, task: &Row) -> Self {
        Self {
            run_id: task.get(0),
            task_status: task.get(1),
            task_generation: task.get(2),
            attempt: task.get(3),
            lease_owner: task.get(4),
            lease_token: task.get(5),
            lease_expires_at: task.get(6),
            run_status: run.get(0),
            run_version: run.get(1),
            execution_generation: run.get(2),
            next_event_sequence: run.get(3),
            checkpoint_sequence: run.get(4),
            workflow_version_id: run.get(5),
            coordinator_agent_version_id: run.get(6),
            deadline: run.get(7),
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

#[allow(clippy::too_many_lines)]
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
            let resume_task_id = uuid(wait.resume_task.task_id.into_bytes());
            let resume_task_kind = task_kind(wait.resume_task.kind);
            let resume_max_attempts = i64::from(wait.resume_task.max_attempts);
            let resume_input = json_value(&wait.resume_task.input)?;
            let resume_deadline = wait.resume_task.deadline.map(UnixMicros::get);
            transaction
                .execute(
                    "INSERT INTO agent_loom.wait_subscriptions (\
                        wait_id, tenant_id, run_id, stage_execution_id, wait_type, \
                        expected_event_type, match_key_hash, match_contract_json, status, \
                        active_slot, expires_at, consumed_by_event_id, created_event_id, \
                        created_at, consumed_at, updated_at, resume_task_id, \
                        resume_logical_key, resume_task_kind, resume_priority, \
                        resume_max_attempts, resume_input_json, resume_deadline\
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open', 1, \
                        to_timestamp(($9::bigint)::double precision / 1000000.0), NULL, $10, \
                        to_timestamp(($11::bigint)::double precision / 1000000.0), NULL, \
                        to_timestamp(($11::bigint)::double precision / 1000000.0), \
                        $12, $13, $14, $15, $16, $17, \
                        to_timestamp(($18::bigint)::double precision / 1000000.0))",
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
                        &resume_task_id,
                        &wait.resume_task.logical_key.as_str(),
                        &resume_task_kind,
                        &wait.resume_task.priority,
                        &resume_max_attempts,
                        &resume_input,
                        &resume_deadline,
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
pub(crate) fn map_database_error(error: tokio_postgres::Error) -> StoreError {
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

pub(crate) fn inconsistent(message: &str) -> StoreError {
    store_error(StoreErrorCode::InconsistentProjection, message)
}

fn json_value(payload: &JsonPayload) -> StoreResult<Value> {
    serde_json::from_slice(payload.as_bytes())
        .map_err(|_| invalid_command("payload is not valid JSON"))
}

fn to_i64(value: u64, field: &str) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| invalid_command(&format!("{field} exceeds database range")))
}

pub(crate) fn nonnegative_u64(value: i64, field: &str) -> StoreResult<u64> {
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

pub(crate) fn uuid(bytes: [u8; 16]) -> Uuid {
    Uuid::from_bytes(bytes)
}

pub(crate) fn run_id_from_uuid(value: Uuid) -> RunId {
    RunId::from_bytes(value.into_bytes())
}

fn task_id_from_uuid(value: Uuid) -> TaskId {
    TaskId::from_bytes(value.into_bytes())
}

fn stage_id_from_uuid(value: Uuid) -> StageExecutionId {
    StageExecutionId::from_bytes(value.into_bytes())
}

pub(crate) fn workflow_id_from_uuid(value: Uuid) -> WorkflowVersionId {
    WorkflowVersionId::from_bytes(value.into_bytes())
}

pub(crate) fn event_id_from_uuid(value: Uuid) -> EventId {
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

fn parse_tool_status(value: &str) -> StoreResult<ToolExecutionStatus> {
    match value {
        "planned" => Ok(ToolExecutionStatus::Planned),
        "executing" => Ok(ToolExecutionStatus::Executing),
        "retry_scheduled" => Ok(ToolExecutionStatus::RetryScheduled),
        "succeeded" => Ok(ToolExecutionStatus::Succeeded),
        "failed" => Ok(ToolExecutionStatus::Failed),
        "outcome_unknown" => Ok(ToolExecutionStatus::OutcomeUnknown),
        "reconciling" => Ok(ToolExecutionStatus::Reconciling),
        "compensated" => Ok(ToolExecutionStatus::Compensated),
        "manual_review" => Ok(ToolExecutionStatus::ManualReview),
        _ => Err(inconsistent(
            "database contains an unknown tool execution status",
        )),
    }
}

const fn tool_status(value: ToolExecutionStatus) -> &'static str {
    match value {
        ToolExecutionStatus::Planned => "planned",
        ToolExecutionStatus::Executing => "executing",
        ToolExecutionStatus::RetryScheduled => "retry_scheduled",
        ToolExecutionStatus::Succeeded => "succeeded",
        ToolExecutionStatus::Failed => "failed",
        ToolExecutionStatus::OutcomeUnknown => "outcome_unknown",
        ToolExecutionStatus::Reconciling => "reconciling",
        ToolExecutionStatus::Compensated => "compensated",
        ToolExecutionStatus::ManualReview => "manual_review",
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

pub(crate) fn parse_run_status(value: &str) -> StoreResult<RunStatus> {
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

fn failure_committed(
    disposition: CommandDisposition,
    snapshot: RunSnapshot,
    event_id: Option<EventId>,
    durable_follow_ups: Vec<DurableFollowUp>,
    wake_scheduler: bool,
) -> Committed<RunSnapshot> {
    let run_id = snapshot.run_id;
    let mut post_commit_hints = vec![
        PostCommitHint::RunEventsAvailable { run_id },
        PostCommitHint::InvalidateRunCache { run_id },
    ];
    if wake_scheduler {
        post_commit_hints.push(PostCommitHint::WakeScheduler);
    }
    Committed {
        disposition,
        value: snapshot,
        event_ids: event_id.into_iter().collect(),
        durable_follow_ups,
        post_commit_hints,
    }
}

fn event_committed(
    disposition: CommandDisposition,
    snapshot: RunSnapshot,
    event_id: Option<EventId>,
    resume_task_id: Option<TaskId>,
) -> Committed<RunSnapshot> {
    let run_id = snapshot.run_id;
    let mut post_commit_hints = vec![
        PostCommitHint::RunEventsAvailable { run_id },
        PostCommitHint::InvalidateRunCache { run_id },
    ];
    if resume_task_id.is_some() {
        post_commit_hints.push(PostCommitHint::WakeWorkers);
    }
    Committed {
        disposition,
        value: snapshot,
        event_ids: event_id.into_iter().collect(),
        durable_follow_ups: resume_task_id
            .map(|task_id| DurableFollowUp::Task { task_id })
            .into_iter()
            .collect(),
        post_commit_hints,
    }
}

fn tool_committed(
    disposition: CommandDisposition,
    snapshot: ToolExecutionSnapshot,
    event_id: Option<EventId>,
    durable_follow_ups: Vec<DurableFollowUp>,
) -> Committed<ToolExecutionSnapshot> {
    let run_id = snapshot.run_id;
    Committed {
        disposition,
        value: snapshot,
        event_ids: event_id.into_iter().collect(),
        durable_follow_ups,
        post_commit_hints: vec![
            PostCommitHint::RunEventsAvailable { run_id },
            PostCommitHint::InvalidateRunCache { run_id },
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
struct ToolReceipt {
    #[serde(rename = "type")]
    outcome_type: String,
    tool_execution_id: Uuid,
    run_id: Uuid,
    stage_execution_id: Option<Uuid>,
    task_id: Uuid,
    tool_call_id: String,
    tool_name: String,
    status: String,
    attempt_count: u32,
    external_ref: Option<String>,
    recovery_action: Option<String>,
    retry_at: Option<i64>,
    updated_at: i64,
}

fn encode_tool_receipt(snapshot: &ToolExecutionSnapshot) -> StoreResult<Value> {
    serde_json::to_value(ToolReceipt {
        outcome_type: "tool_execution".to_owned(),
        tool_execution_id: uuid(snapshot.tool_execution_id.into_bytes()),
        run_id: uuid(snapshot.run_id.into_bytes()),
        stage_execution_id: snapshot
            .stage_execution_id
            .map(|value| uuid(value.into_bytes())),
        task_id: uuid(snapshot.task_id.into_bytes()),
        tool_call_id: snapshot.tool_call_id.clone(),
        tool_name: snapshot.tool_name.clone(),
        status: tool_status(snapshot.status).to_owned(),
        attempt_count: snapshot.attempt_count,
        external_ref: snapshot.external_ref.clone(),
        recovery_action: snapshot.recovery_action.clone(),
        retry_at: snapshot.retry_at.map(UnixMicros::get),
        updated_at: snapshot.updated_at.get(),
    })
    .map_err(|_| inconsistent("failed to encode ToolExecution receipt"))
}

fn decode_tool_receipt(tenant_id: TenantId, value: &Value) -> StoreResult<ToolExecutionSnapshot> {
    let receipt: ToolReceipt = serde_json::from_value(value.clone())
        .map_err(|_| inconsistent("stored command receipt is not a ToolExecution outcome"))?;
    if receipt.outcome_type != "tool_execution" {
        return Err(inconsistent(
            "stored command receipt has the wrong outcome type",
        ));
    }
    Ok(ToolExecutionSnapshot {
        tenant_id,
        tool_execution_id: ToolExecutionId::from_bytes(receipt.tool_execution_id.into_bytes()),
        run_id: run_id_from_uuid(receipt.run_id),
        stage_execution_id: receipt.stage_execution_id.map(stage_id_from_uuid),
        task_id: task_id_from_uuid(receipt.task_id),
        tool_call_id: receipt.tool_call_id,
        tool_name: receipt.tool_name,
        status: parse_tool_status(&receipt.status)?,
        attempt_count: receipt.attempt_count,
        external_ref: receipt.external_ref,
        recovery_action: receipt.recovery_action,
        retry_at: receipt.retry_at.map(UnixMicros::new),
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
        EventCursor, ExpectedRun, FinalRunResult, LeaseProof, NewCheckpoint, NewWaitSubscription,
        QueryContext, TaskResult, WaitResumeTask,
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
    fn control_no_op_rules_preserve_terminal_runs() {
        assert!(ControlKind::Pause.is_no_op(RunStatus::Paused));
        assert!(ControlKind::Pause.is_no_op(RunStatus::Completed));
        assert!(ControlKind::Cancel.is_no_op(RunStatus::Cancelled));
        assert!(ControlKind::Cancel.is_no_op(RunStatus::Completed));
        assert!(!ControlKind::Cancel.is_no_op(RunStatus::Running));
        assert!(!ControlKind::Resume.is_no_op(RunStatus::Paused));
    }

    #[test]
    fn task_failure_projection_distinguishes_retry_fatal_and_dead_letter() {
        let retry = FailureTransition::classify(1, 3, Some(99));
        assert_eq!(retry.task_status, "retry_scheduled");
        assert_eq!(retry.run_status, "retrying");
        assert_eq!(retry.retry_at, Some(99));
        assert!(!retry.terminal);

        let fatal = FailureTransition::classify(1, 3, None);
        assert_eq!(fatal.task_status, "failed");
        assert_eq!(fatal.run_status, "failed");
        assert!(fatal.terminal);

        let dead_letter = FailureTransition::classify(3, 3, Some(99));
        assert_eq!(dead_letter.task_status, "dead_lettered");
        assert_eq!(dead_letter.run_status, "waiting");
        assert_eq!(dead_letter.retry_at, None);
        assert!(!dead_letter.terminal);
    }

    #[test]
    fn external_event_rejects_failed_verification_and_missing_schema() {
        let mut command = ApplyEvent {
            expected_run: ExpectedRun {
                run_id: RunId::from_bytes([1; 16]),
                version: None,
                execution_generation: None,
            },
            event_id: EventId::from_bytes([2; 16]),
            event_type: "approval.granted".to_owned(),
            match_key_hash: Digest::from_bytes([3; 32]),
            payload_schema_version: 1,
            payload: payload(&json!({"approved": true})),
            signature_verification: SignatureVerification::Failed,
            occurred_at: None,
        };
        assert_eq!(
            validate_apply_event(&command)
                .expect_err("failed signature is rejected")
                .code,
            StoreErrorCode::ConstraintViolation
        );
        command.signature_verification = SignatureVerification::Verified;
        command.payload_schema_version = 0;
        assert_eq!(
            validate_apply_event(&command)
                .expect_err("missing schema is rejected")
                .code,
            StoreErrorCode::ConstraintViolation
        );
    }

    #[test]
    fn wait_match_contract_checks_required_and_equal_fields() {
        let contract = json!({
            "required": ["approved", "reviewer"],
            "equals": {"approved": true}
        });
        assert_eq!(
            validate_match_contract(&contract, &json!({"approved": true, "reviewer": "alice"})),
            Ok(())
        );
        assert_eq!(
            validate_match_contract(&contract, &json!({"approved": false}))
                .expect_err("mismatched payload is rejected")
                .code,
            StoreErrorCode::WaitMismatch
        );
    }

    #[test]
    fn tool_retry_projection_requires_a_durable_due_time() {
        let retry_at = UnixMicros::new(123);
        let projected = project_tool_outcome(&ToolRecordedOutcome::Failed {
            error_code: "busy".to_owned(),
            retry: ExecutionRetryClass::SameRequestBackoff,
            retry_at: Some(retry_at),
        })
        .expect("valid retry projection");
        assert_eq!(projected.status, ToolExecutionStatus::RetryScheduled);
        assert_eq!(projected.retry_at, Some(123));

        let error = project_tool_outcome(&ToolRecordedOutcome::Failed {
            error_code: "busy".to_owned(),
            retry: ExecutionRetryClass::SameRequestBackoff,
            retry_at: None,
        })
        .expect_err("in-memory-only retry is rejected");
        assert_eq!(error.code, StoreErrorCode::ConstraintViolation);
    }

    #[test]
    fn tool_receipt_round_trip_preserves_retry_schedule() {
        let snapshot = ToolExecutionSnapshot {
            tenant_id: TenantId::from_bytes([1; 16]),
            tool_execution_id: ToolExecutionId::from_bytes([2; 16]),
            run_id: RunId::from_bytes([3; 16]),
            stage_execution_id: None,
            task_id: TaskId::from_bytes([4; 16]),
            tool_call_id: "deploy-1".to_owned(),
            tool_name: "devops.deploy".to_owned(),
            status: ToolExecutionStatus::RetryScheduled,
            attempt_count: 2,
            external_ref: Some("job-1".to_owned()),
            recovery_action: Some("retry_same_request".to_owned()),
            retry_at: Some(UnixMicros::new(500)),
            updated_at: UnixMicros::new(400),
        };
        let encoded = encode_tool_receipt(&snapshot).expect("encode Tool receipt");
        assert_eq!(
            decode_tool_receipt(snapshot.tenant_id, &encoded).expect("decode Tool receipt"),
            snapshot
        );
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
        let receipt_guard = [
            "ON CONFLICT ",
            "(tenant_id, scope, idempotency_key)",
            " DO NOTHING",
        ]
        .concat();
        let joined_lock = ["FOR UPDATE OF ", "t, r", " SKIP LOCKED"].concat();
        let generation_cas = ["AND execution_generation", " = $4"].concat();
        let receipt_finish = ["AND outcome_kind", " = 'outcome_unknown'"].concat();
        let expiry_fence = ["expires", " <= ", "db_now"].concat();
        assert!(source.contains(&receipt_guard));
        assert!(!source.contains(&joined_lock));
        assert!(source.contains("FROM agent_loom.runs"));
        assert!(source.contains("FROM agent_loom.tasks"));
        assert!(source.contains(&generation_cas));
        assert!(source.contains(&receipt_finish));
        assert!(source.contains(&expiry_fence));
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

        let query_context = QueryContext {
            tenant_id,
            actor_ref: "postgres-smoke-test".to_owned(),
            authoritative: true,
        };
        let queried = executor
            .get_run(&client, &query_context, run_id)
            .await
            .expect("query completed run")
            .expect("completed run exists");
        assert_eq!(queried, completed.value);
        let events = executor
            .list_events(
                &client,
                &query_context,
                EventCursor {
                    run_id,
                    after_sequence: 0,
                    limit: 10,
                },
            )
            .await
            .expect("list run events");
        assert_eq!(events.events.len(), 3);
        assert_eq!(events.events[0].sequence, 1);
        assert_eq!(events.events[2].sequence, 3);

        let recovery_run_id = RunId::from_bytes(ids(50));
        let recovery_task_id = TaskId::from_bytes(ids(51));
        let recovery_initial_event = EventId::from_bytes(ids(52));
        executor
            .create_run(
                &mut client,
                &command_context(tenant_id, ids(54), "create_run", &tenant_key, 54),
                smoke_run(
                    recovery_run_id,
                    recovery_task_id,
                    recovery_initial_event,
                    CheckpointId::from_bytes(ids(53)),
                    now_micros,
                    "worker-recovery",
                ),
            )
            .await
            .expect("create worker recovery run");
        let recovery_worker = WorkerId::from_bytes(ids(55));
        let recovery_token = LeaseToken::from_bytes([56; 32]);
        let recovery_claim = executor
            .claim_task(
                &mut client,
                &command_context(tenant_id, ids(56), "claim_task", &tenant_key, 56),
                ClaimTask {
                    worker_id: recovery_worker,
                    lease_token: recovery_token.clone(),
                    lease_duration: DurationMicros::new(60_000_000),
                    candidate_window: 8,
                },
            )
            .await
            .expect("claim worker recovery task")
            .expect("worker recovery task is claimable");
        let renew_context =
            command_context(tenant_id, ids(57), "renew_task_lease", &tenant_key, 57);
        let renewal = RenewTaskLease {
            expected_run: ExpectedRun {
                run_id: recovery_run_id,
                version: Some(1),
                execution_generation: Some(0),
            },
            lease: LeaseProof {
                task_id: recovery_task_id,
                worker_id: recovery_worker,
                token: recovery_token.clone(),
                execution_generation: 0,
            },
            extension: DurationMicros::new(30_000_000),
        };
        let renewed = executor
            .renew_task_lease(&mut client, &renew_context, renewal.clone())
            .await
            .expect("renew task lease");
        assert_eq!(
            renewed.value.lease_expires_at.get(),
            recovery_claim.value.lease_expires_at.get() + 30_000_000
        );
        let renewed_duplicate = executor
            .renew_task_lease(&mut client, &renew_context, renewal)
            .await
            .expect("replay task lease renewal");
        assert_eq!(renewed_duplicate.disposition, CommandDisposition::Duplicate);
        assert_eq!(renewed_duplicate.value, renewed.value);

        let fail_context = command_context(tenant_id, ids(58), "fail_task", &tenant_key, 58);
        let retry_failure = FailTask {
            expected_run: ExpectedRun {
                run_id: recovery_run_id,
                version: Some(1),
                execution_generation: Some(0),
            },
            lease: LeaseProof {
                task_id: recovery_task_id,
                worker_id: recovery_worker,
                token: recovery_token.clone(),
                execution_generation: 0,
            },
            failure_event_id: EventId::from_bytes(ids(59)),
            error_code: "transient_worker_error".to_owned(),
            retry_at: Some(UnixMicros::new(now_micros)),
        };
        let retrying = executor
            .fail_task(&mut client, &fail_context, retry_failure.clone())
            .await
            .expect("schedule task retry");
        assert_eq!(retrying.value.status, RunStatus::Retrying);
        assert_eq!(retrying.value.version, 2);
        assert_eq!(retrying.durable_follow_ups.len(), 1);
        let retry_duplicate = executor
            .fail_task(&mut client, &fail_context, retry_failure)
            .await
            .expect("replay task failure");
        assert_eq!(retry_duplicate.disposition, CommandDisposition::Duplicate);
        assert_eq!(retry_duplicate.value, retrying.value);
        let recovery_task_status: String = client
            .query_one(
                "SELECT status FROM agent_loom.tasks \
                 WHERE tenant_id = $1 AND task_id = $2",
                &[&tenant_uuid, &uuid(recovery_task_id.into_bytes())],
            )
            .await
            .expect("query retry task")
            .get(0);
        assert_eq!(recovery_task_status, "retry_scheduled");
        let stale_renewal = executor
            .renew_task_lease(
                &mut client,
                &command_context(tenant_id, ids(69), "renew_task_lease", &tenant_key, 69),
                RenewTaskLease {
                    expected_run: ExpectedRun {
                        run_id: recovery_run_id,
                        version: Some(2),
                        execution_generation: Some(0),
                    },
                    lease: LeaseProof {
                        task_id: recovery_task_id,
                        worker_id: recovery_worker,
                        token: recovery_token,
                        execution_generation: 0,
                    },
                    extension: DurationMicros::new(1_000_000),
                },
            )
            .await
            .expect_err("finalized lease cannot be renewed");
        assert_eq!(stale_renewal.code, StoreErrorCode::LeaseLost);

        let fatal_run_id = RunId::from_bytes(ids(60));
        let fatal_task_id = TaskId::from_bytes(ids(61));
        executor
            .create_run(
                &mut client,
                &command_context(tenant_id, ids(64), "create_run", &tenant_key, 64),
                smoke_run(
                    fatal_run_id,
                    fatal_task_id,
                    EventId::from_bytes(ids(62)),
                    CheckpointId::from_bytes(ids(63)),
                    now_micros,
                    "fatal-worker-failure",
                ),
            )
            .await
            .expect("create fatal failure run");
        let fatal_worker = WorkerId::from_bytes(ids(65));
        let fatal_token = LeaseToken::from_bytes([66; 32]);
        executor
            .claim_task(
                &mut client,
                &command_context(tenant_id, ids(66), "claim_task", &tenant_key, 66),
                ClaimTask {
                    worker_id: fatal_worker,
                    lease_token: fatal_token.clone(),
                    lease_duration: DurationMicros::new(60_000_000),
                    candidate_window: 8,
                },
            )
            .await
            .expect("claim fatal failure task")
            .expect("fatal failure task is claimable");
        let failed = executor
            .fail_task(
                &mut client,
                &command_context(tenant_id, ids(67), "fail_task", &tenant_key, 67),
                FailTask {
                    expected_run: ExpectedRun {
                        run_id: fatal_run_id,
                        version: Some(1),
                        execution_generation: Some(0),
                    },
                    lease: LeaseProof {
                        task_id: fatal_task_id,
                        worker_id: fatal_worker,
                        token: fatal_token,
                        execution_generation: 0,
                    },
                    failure_event_id: EventId::from_bytes(ids(68)),
                    error_code: "fatal_worker_error".to_owned(),
                    retry_at: None,
                },
            )
            .await
            .expect("fail run from non-retryable task failure");
        assert_eq!(failed.value.status, RunStatus::Failed);
        assert!(failed.value.terminal_invariant_holds());

        let wait_run_id = RunId::from_bytes(ids(70));
        let wait_task_id = TaskId::from_bytes(ids(71));
        executor
            .create_run(
                &mut client,
                &command_context(tenant_id, ids(74), "create_run", &tenant_key, 74),
                smoke_run(
                    wait_run_id,
                    wait_task_id,
                    EventId::from_bytes(ids(72)),
                    CheckpointId::from_bytes(ids(73)),
                    now_micros,
                    "approval-wait",
                ),
            )
            .await
            .expect("create approval wait run");
        let wait_worker = WorkerId::from_bytes(ids(75));
        let wait_token = LeaseToken::from_bytes([76; 32]);
        executor
            .claim_task(
                &mut client,
                &command_context(tenant_id, ids(76), "claim_task", &tenant_key, 76),
                ClaimTask {
                    worker_id: wait_worker,
                    lease_token: wait_token.clone(),
                    lease_duration: DurationMicros::new(60_000_000),
                    candidate_window: 8,
                },
            )
            .await
            .expect("claim approval wait task")
            .expect("approval wait task is claimable");
        let wait_completion_event = EventId::from_bytes(ids(77));
        let match_key_hash = Digest::from_bytes([81; 32]);
        let resume_task_id = TaskId::from_bytes(ids(82));
        let waiting = executor
            .complete_task(
                &mut client,
                &command_context(tenant_id, ids(79), "complete_task", &tenant_key, 79),
                CompleteTask {
                    expected_run: ExpectedRun {
                        run_id: wait_run_id,
                        version: Some(1),
                        execution_generation: Some(0),
                    },
                    lease: LeaseProof {
                        task_id: wait_task_id,
                        worker_id: wait_worker,
                        token: wait_token,
                        execution_generation: 0,
                    },
                    completion_event_id: wait_completion_event,
                    checkpoint: NewCheckpoint {
                        checkpoint_id: CheckpointId::from_bytes(ids(78)),
                        sequence: 2,
                        schema_version: 1,
                        workflow_version_id: None,
                        coordinator_agent_version_id: None,
                        execution_generation: 0,
                        state: payload(&json!({"waiting_for": "approval"})),
                        state_digest: Digest::from_bytes([78; 32]),
                        created_event_id: wait_completion_event,
                    },
                    task_result: TaskResult {
                        output: payload(&json!({"proposal": "ready"})),
                    },
                    stage_mutation: None,
                    artifacts: Vec::new(),
                    next: NextActions::Wait(NewWaitSubscription {
                        wait_id: agent_loom_domain::WaitId::from_bytes(ids(80)),
                        stage_execution_id: None,
                        wait_type: "approval".to_owned(),
                        expected_event_type: "approval.granted".to_owned(),
                        match_key_hash,
                        match_contract: payload(&json!({"required": ["approved"]})),
                        expires_at: None,
                        resume_task: WaitResumeTask {
                            task_id: resume_task_id,
                            logical_key: LogicalKey::parse("smoke/approval-resume")
                                .expect("logical key"),
                            kind: TaskKind::Model,
                            priority: 10,
                            max_attempts: 2,
                            input: payload(&json!({"instruction": "continue delivery"})),
                            deadline: None,
                        },
                        created_event_id: wait_completion_event,
                    }),
                },
            )
            .await
            .expect("enter approval wait");
        assert_eq!(waiting.value.status, RunStatus::ApprovalRequired);

        let paused_wait_run = executor
            .pause_run(
                &mut client,
                &command_context(tenant_id, ids(91), "pause_run", &tenant_key, 91),
                ControlRun {
                    expected_run: ExpectedRun {
                        run_id: wait_run_id,
                        version: Some(2),
                        execution_generation: Some(0),
                    },
                    event_id: EventId::from_bytes(ids(92)),
                    reason: "pause while awaiting approval".to_owned(),
                },
            )
            .await
            .expect("pause approval wait run");
        assert_eq!(paused_wait_run.value.status, RunStatus::Paused);
        assert_eq!(paused_wait_run.value.execution_generation, 1);

        let apply_context = command_context(tenant_id, ids(83), "apply_event", &tenant_key, 83);
        let external_event = ApplyEvent {
            expected_run: ExpectedRun {
                run_id: wait_run_id,
                version: Some(3),
                execution_generation: Some(1),
            },
            event_id: EventId::from_bytes(ids(84)),
            event_type: "approval.granted".to_owned(),
            match_key_hash,
            payload_schema_version: 2,
            payload: payload(&json!({"approved": true, "reviewer": "smoke"})),
            signature_verification: SignatureVerification::Verified,
            occurred_at: Some(UnixMicros::new(now_micros)),
        };
        let applied_event = executor
            .apply_event(&mut client, &apply_context, external_event.clone())
            .await
            .expect("consume approval event");
        assert_eq!(applied_event.value.status, RunStatus::Paused);
        assert_eq!(applied_event.value.version, 4);
        assert!(applied_event.durable_follow_ups.is_empty());
        let event_duplicate = executor
            .apply_event(&mut client, &apply_context, external_event)
            .await
            .expect("replay approval event");
        assert_eq!(event_duplicate.disposition, CommandDisposition::Duplicate);
        assert_eq!(event_duplicate.value, applied_event.value);
        let second_event_error = executor
            .apply_event(
                &mut client,
                &command_context(tenant_id, ids(85), "apply_event", &tenant_key, 85),
                ApplyEvent {
                    expected_run: ExpectedRun {
                        run_id: wait_run_id,
                        version: Some(4),
                        execution_generation: Some(1),
                    },
                    event_id: EventId::from_bytes(ids(86)),
                    event_type: "approval.granted".to_owned(),
                    match_key_hash,
                    payload_schema_version: 2,
                    payload: payload(&json!({"approved": true})),
                    signature_verification: SignatureVerification::Verified,
                    occurred_at: None,
                },
            )
            .await
            .expect_err("a wait can only be consumed once");
        assert_eq!(second_event_error.code, StoreErrorCode::WaitAlreadyConsumed);
        let resumed_task_status: String = client
            .query_one(
                "SELECT status FROM agent_loom.tasks \
                 WHERE tenant_id = $1 AND task_id = $2",
                &[&tenant_uuid, &uuid(resume_task_id.into_bytes())],
            )
            .await
            .expect("query resume task")
            .get(0);
        assert_eq!(resumed_task_status, "scheduled");
        let applied_schema_version: i64 = client
            .query_one(
                "SELECT payload_schema_version FROM agent_loom.events \
                 WHERE tenant_id = $1 AND event_id = $2",
                &[&tenant_uuid, &uuid(ids(84))],
            )
            .await
            .expect("query applied event")
            .get(0);
        assert_eq!(applied_schema_version, 2);
        let resumed_wait_run = executor
            .resume_run(
                &mut client,
                &command_context(tenant_id, ids(93), "resume_run", &tenant_key, 93),
                ControlRun {
                    expected_run: ExpectedRun {
                        run_id: wait_run_id,
                        version: Some(4),
                        execution_generation: Some(1),
                    },
                    event_id: EventId::from_bytes(ids(94)),
                    reason: "resume after approval arrived".to_owned(),
                },
            )
            .await
            .expect("resume approval run");
        assert_eq!(resumed_wait_run.value.status, RunStatus::Queued);
        let resumed_claim = executor
            .claim_task(
                &mut client,
                &command_context(tenant_id, ids(87), "claim_task", &tenant_key, 87),
                ClaimTask {
                    worker_id: WorkerId::from_bytes(ids(87)),
                    lease_token: LeaseToken::from_bytes([88; 32]),
                    lease_duration: DurationMicros::new(60_000_000),
                    candidate_window: 8,
                },
            )
            .await
            .expect("claim resume task")
            .expect("resume task is claimable");
        assert_eq!(resumed_claim.value.task.task_id, resume_task_id);
        let cancelled_wait_run = executor
            .cancel_run(
                &mut client,
                &command_context(tenant_id, ids(89), "cancel_run", &tenant_key, 89),
                ControlRun {
                    expected_run: ExpectedRun {
                        run_id: wait_run_id,
                        version: Some(6),
                        execution_generation: Some(1),
                    },
                    event_id: EventId::from_bytes(ids(90)),
                    reason: "finish apply-event smoke path".to_owned(),
                },
            )
            .await
            .expect("cancel apply-event smoke run");
        assert_eq!(cancelled_wait_run.value.status, RunStatus::Cancelled);

        let control_run_id = RunId::from_bytes(ids(12));
        let control_task_id = TaskId::from_bytes(ids(13));
        let control_initial_event = EventId::from_bytes(ids(14));
        let control_create_context =
            command_context(tenant_id, ids(16), "create_run", &tenant_key, 16);
        executor
            .create_run(
                &mut client,
                &control_create_context,
                CreateRun {
                    run_id: control_run_id,
                    workflow_version_id: None,
                    coordinator_agent_version_id: None,
                    input: payload(&json!({"request": "control-smoke"})),
                    deadline: None,
                    initial_event_id: control_initial_event,
                    initial_checkpoint: NewCheckpoint {
                        checkpoint_id: CheckpointId::from_bytes(ids(15)),
                        sequence: 1,
                        schema_version: 1,
                        workflow_version_id: None,
                        coordinator_agent_version_id: None,
                        execution_generation: 0,
                        state: payload(&json!({"step": 1})),
                        state_digest: Digest::from_bytes([15; 32]),
                        created_event_id: control_initial_event,
                    },
                    initial_tasks: vec![InitialTask {
                        task_id: control_task_id,
                        stage_execution_id: None,
                        logical_key: LogicalKey::parse("smoke/control-task").expect("logical key"),
                        kind: TaskKind::Model,
                        priority: 10,
                        available_at: UnixMicros::new(now_micros),
                        max_attempts: 3,
                        input: payload(&json!({"prompt": "control-smoke"})),
                    }],
                },
            )
            .await
            .expect("create control run");

        let pause_context = command_context(tenant_id, ids(17), "pause_run", &tenant_key, 17);
        let pause = ControlRun {
            expected_run: ExpectedRun {
                run_id: control_run_id,
                version: Some(0),
                execution_generation: Some(0),
            },
            event_id: EventId::from_bytes(ids(18)),
            reason: "operator pause".to_owned(),
        };
        let paused = executor
            .pause_run(&mut client, &pause_context, pause.clone())
            .await
            .expect("pause run");
        assert_eq!(paused.value.status, RunStatus::Paused);
        assert_eq!(paused.value.execution_generation, 1);
        let pause_duplicate = executor
            .pause_run(&mut client, &pause_context, pause)
            .await
            .expect("replay pause");
        assert_eq!(pause_duplicate.disposition, CommandDisposition::Duplicate);
        assert_eq!(pause_duplicate.value, paused.value);

        let resume_context = command_context(tenant_id, ids(19), "resume_run", &tenant_key, 19);
        let resumed = executor
            .resume_run(
                &mut client,
                &resume_context,
                ControlRun {
                    expected_run: ExpectedRun {
                        run_id: control_run_id,
                        version: Some(1),
                        execution_generation: Some(1),
                    },
                    event_id: EventId::from_bytes(ids(20)),
                    reason: "operator resume".to_owned(),
                },
            )
            .await
            .expect("resume run");
        assert_eq!(resumed.value.status, RunStatus::Queued);
        assert_eq!(resumed.value.execution_generation, 1);

        let cancel_context = command_context(tenant_id, ids(21), "cancel_run", &tenant_key, 21);
        let cancelled = executor
            .cancel_run(
                &mut client,
                &cancel_context,
                ControlRun {
                    expected_run: ExpectedRun {
                        run_id: control_run_id,
                        version: Some(2),
                        execution_generation: Some(1),
                    },
                    event_id: EventId::from_bytes(ids(22)),
                    reason: "operator cancel".to_owned(),
                },
            )
            .await
            .expect("cancel run");
        assert_eq!(cancelled.value.status, RunStatus::Cancelled);
        assert_eq!(cancelled.value.execution_generation, 2);
        assert!(cancelled.value.terminal_invariant_holds());

        let cancel_no_op_context =
            command_context(tenant_id, ids(23), "cancel_run", &tenant_key, 23);
        let cancel_no_op = executor
            .cancel_run(
                &mut client,
                &cancel_no_op_context,
                ControlRun {
                    expected_run: ExpectedRun {
                        run_id: control_run_id,
                        version: Some(0),
                        execution_generation: Some(0),
                    },
                    event_id: EventId::from_bytes(ids(24)),
                    reason: "duplicate terminal cancel".to_owned(),
                },
            )
            .await
            .expect("terminal cancel is a no-op");
        assert_eq!(cancel_no_op.disposition, CommandDisposition::NoOp);
        assert_eq!(cancel_no_op.value, cancelled.value);

        let race_run_id = RunId::from_bytes(ids(25));
        let race_task_id = TaskId::from_bytes(ids(26));
        let race_initial_event = EventId::from_bytes(ids(27));
        let race_create_context =
            command_context(tenant_id, ids(29), "create_run", &tenant_key, 29);
        executor
            .create_run(
                &mut client,
                &race_create_context,
                smoke_run(
                    race_run_id,
                    race_task_id,
                    race_initial_event,
                    CheckpointId::from_bytes(ids(28)),
                    now_micros,
                    "race",
                ),
            )
            .await
            .expect("create race run");
        let race_worker = WorkerId::from_bytes(ids(31));
        let race_token = LeaseToken::from_bytes([32; 32]);
        let race_claim_context = command_context(tenant_id, ids(30), "claim_task", &tenant_key, 30);
        executor
            .claim_task(
                &mut client,
                &race_claim_context,
                ClaimTask {
                    worker_id: race_worker,
                    lease_token: race_token.clone(),
                    lease_duration: DurationMicros::new(60_000_000),
                    candidate_window: 8,
                },
            )
            .await
            .expect("claim race task")
            .expect("race task is claimable");

        let (mut cancel_client, cancel_connection) =
            tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .expect("connect cancellation race client");
        let cancel_connection_task = tokio::spawn(cancel_connection);
        let race_completion_event = EventId::from_bytes(ids(34));
        let race_complete_context =
            command_context(tenant_id, ids(33), "complete_task", &tenant_key, 33);
        let race_cancel_context =
            command_context(tenant_id, ids(36), "cancel_run", &tenant_key, 36);
        let complete_future = executor.complete_task(
            &mut client,
            &race_complete_context,
            CompleteTask {
                expected_run: ExpectedRun {
                    run_id: race_run_id,
                    version: Some(1),
                    execution_generation: Some(0),
                },
                lease: LeaseProof {
                    task_id: race_task_id,
                    worker_id: race_worker,
                    token: race_token,
                    execution_generation: 0,
                },
                completion_event_id: race_completion_event,
                checkpoint: NewCheckpoint {
                    checkpoint_id: CheckpointId::from_bytes(ids(35)),
                    sequence: 2,
                    schema_version: 1,
                    workflow_version_id: None,
                    coordinator_agent_version_id: None,
                    execution_generation: 0,
                    state: payload(&json!({"step": 2})),
                    state_digest: Digest::from_bytes([35; 32]),
                    created_event_id: race_completion_event,
                },
                task_result: TaskResult {
                    output: payload(&json!({"ok": true})),
                },
                stage_mutation: None,
                artifacts: Vec::new(),
                next: NextActions::FinishRun(FinalRunResult {
                    status: RunStatus::Completed,
                    output: payload(&json!({"winner": "complete"})),
                }),
            },
        );
        let cancel_future = executor.cancel_run(
            &mut cancel_client,
            &race_cancel_context,
            ControlRun {
                expected_run: ExpectedRun {
                    run_id: race_run_id,
                    version: Some(1),
                    execution_generation: Some(0),
                },
                event_id: EventId::from_bytes(ids(37)),
                reason: "cancel/complete race".to_owned(),
            },
        );
        let (complete_result, cancel_result) = tokio::join!(complete_future, cancel_future);
        match (complete_result, cancel_result) {
            (Ok(completed), Ok(cancelled)) => {
                assert_eq!(completed.value.status, RunStatus::Completed);
                assert_eq!(cancelled.disposition, CommandDisposition::NoOp);
                assert_eq!(cancelled.value.status, RunStatus::Completed);
            }
            (Err(completion_error), Ok(cancelled)) => {
                assert!(matches!(
                    completion_error.code,
                    StoreErrorCode::LeaseLost | StoreErrorCode::TerminalRun
                ));
                assert_eq!(cancelled.disposition, CommandDisposition::Applied);
                assert_eq!(cancelled.value.status, RunStatus::Cancelled);
            }
            (Ok(_), Err(error)) | (Err(error), Err(_)) => {
                panic!("cancel/complete race returned an unexpected error: {error:?}");
            }
        }
        let terminal = executor
            .get_run(&client, &query_context, race_run_id)
            .await
            .expect("query race run")
            .expect("race run exists");
        assert!(matches!(
            terminal.status,
            RunStatus::Completed | RunStatus::Cancelled
        ));
        assert!(terminal.terminal_invariant_holds());
        let terminal_event_count: i64 = client
            .query_one(
                "SELECT count(*) FROM agent_loom.events \
                 WHERE tenant_id = $1 AND run_id = $2 \
                   AND event_type IN ('task.completed', 'run.cancelled')",
                &[&tenant_uuid, &uuid(race_run_id.into_bytes())],
            )
            .await
            .expect("count race terminal events")
            .get(0);
        assert_eq!(terminal_event_count, 1);

        drop(cancel_client);
        cancel_connection_task
            .await
            .expect("cancel connection task joins")
            .expect("cancel PostgreSQL connection stays healthy");

        drop(client);
        connection_task
            .await
            .expect("connection task joins")
            .expect("PostgreSQL connection stays healthy");
    }

    fn payload(value: &Value) -> JsonPayload {
        JsonPayload::from_validated_bytes(serde_json::to_vec(&value).expect("serialize payload"))
    }

    fn smoke_run(
        run_id: RunId,
        task_id: TaskId,
        event_id: EventId,
        checkpoint_id: CheckpointId,
        now_micros: i64,
        label: &str,
    ) -> CreateRun {
        CreateRun {
            run_id,
            workflow_version_id: None,
            coordinator_agent_version_id: None,
            input: payload(&json!({"request": label})),
            deadline: None,
            initial_event_id: event_id,
            initial_checkpoint: NewCheckpoint {
                checkpoint_id,
                sequence: 1,
                schema_version: 1,
                workflow_version_id: None,
                coordinator_agent_version_id: None,
                execution_generation: 0,
                state: payload(&json!({"step": 1})),
                state_digest: Digest::from_bytes([28; 32]),
                created_event_id: event_id,
            },
            initial_tasks: vec![InitialTask {
                task_id,
                stage_execution_id: None,
                logical_key: LogicalKey::parse(format!("smoke/{label}-task")).expect("logical key"),
                kind: TaskKind::Model,
                priority: 10,
                available_at: UnixMicros::new(now_micros),
                max_attempts: 3,
                input: payload(&json!({"prompt": label})),
            }],
        }
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
