use agent_loom_domain::{
    AgentExecutionId, AgentExecutionSnapshot, AgentVersionId, ArtifactId, ArtifactRefSnapshot,
    ArtifactVersionRef, CheckpointId, ContextSnapshot, ContextSnapshotId, Digest, EndpointId,
    EventId, EventRecord, IdempotencyKey, JsonPayload, LogicalKey, PlanRevisionId,
    PlanRevisionSnapshot, RunId, RunSnapshot, ScopeKey, StageExecutionId, StageExecutionSnapshot,
    StageStatus, TaskContextReference, TaskId, TenantId, ToolExecutionId, UnixMicros, WaitId,
    WaitSnapshot, WaitStatus, WorkflowId, WorkflowSnapshot, WorkflowVersionId,
};
use agent_loom_durable_store::{
    AgentEventCandidate, AgentEventPage, AgentEventQuery, AgentInvocation, AgentStatusCandidate,
    AgentStatusPage, AgentStatusQuery, AgentStopCandidate, AgentStopPage, AgentStopQuery,
    DueWorkCandidate, DueWorkKind, DueWorkPage, DueWorkQuery, DueWorkTarget, EventCursor,
    EventPage, MaintenanceCandidate, MaintenanceKind, MaintenancePage, MaintenanceQuery,
    MaintenanceTarget, QueryContext, StoreError, StoreErrorCode, StoreResult, ToolInvocation,
};
use serde_json::Value;
use tokio_postgres::{Client, Row};
use uuid::Uuid;

use crate::{
    event_id_from_uuid, inconsistent, map_database_error, nonnegative_u64, parse_run_status,
    run_id_from_uuid, uuid, workflow_id_from_uuid,
};

const MAX_EVENT_PAGE_SIZE: u32 = 1_000;
const MAX_DUE_WORK_PAGE_SIZE: u32 = 1_000;
const MAX_MAINTENANCE_PAGE_SIZE: u32 = 1_000;
const MAX_AGENT_STOP_PAGE_SIZE: u32 = 1_000;
const MAX_AGENT_STATUS_PAGE_SIZE: u32 = 1_000;
const MAX_AGENT_EVENT_PAGE_SIZE: u32 = 1_000;

impl crate::PostgresTransactionExecutor {
    /// Reads the authoritative Run projection for one tenant.
    ///
    /// # Errors
    ///
    /// Returns a stable store error if PostgreSQL is unavailable or a stored
    /// projection violates the shared domain model.
    pub async fn get_run(
        &self,
        client: &Client,
        context: &QueryContext,
        run_id: RunId,
    ) -> StoreResult<Option<RunSnapshot>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_id = uuid(run_id.into_bytes());
        let row = client
            .query_opt(
                "SELECT run_id, workflow_version_id, status, suspended_from_status, version, \
                        execution_generation, next_event_sequence, current_checkpoint_id, \
                        terminal_event_id, \
                        CASE WHEN deadline IS NULL THEN NULL \
                             ELSE (extract(epoch FROM deadline) * 1000000)::bigint END, \
                        (extract(epoch FROM updated_at) * 1000000)::bigint \
                 FROM agent_loom.runs WHERE tenant_id = $1 AND run_id = $2",
                &[&tenant_id, &run_id],
            )
            .await
            .map_err(map_database_error)?;
        row.map(|row| decode_run(context.tenant_id, &row))
            .transpose()
    }

    /// Reads the immutable input bound to a Run.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or malformed
    /// persisted JSON.
    pub async fn get_run_input(
        &self,
        client: &Client,
        context: &QueryContext,
        run_id: RunId,
    ) -> StoreResult<Option<JsonPayload>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_id = uuid(run_id.into_bytes());
        client
            .query_opt(
                "SELECT input_json FROM agent_loom.runs WHERE tenant_id = $1 AND run_id = $2",
                &[&tenant_id, &run_id],
            )
            .await
            .map_err(map_database_error)?
            .map(|row| decode_json_payload(&row.get::<_, Value>(0)))
            .transpose()
    }

    /// Lists direct Child Runs in stable creation order.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or malformed
    /// persisted Run metadata.
    pub async fn list_child_runs(
        &self,
        client: &Client,
        context: &QueryContext,
        parent_run_id: RunId,
    ) -> StoreResult<Vec<RunSnapshot>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let parent_run_id = uuid(parent_run_id.into_bytes());
        client
            .query(
                "SELECT run_id, workflow_version_id, status, suspended_from_status, version, \
                        execution_generation, next_event_sequence, current_checkpoint_id, \
                        terminal_event_id, \
                        CASE WHEN deadline IS NULL THEN NULL \
                             ELSE (extract(epoch FROM deadline) * 1000000)::bigint END, \
                        (extract(epoch FROM updated_at) * 1000000)::bigint \
                 FROM agent_loom.runs WHERE tenant_id = $1 AND parent_run_id = $2 \
                 ORDER BY created_at, run_id",
                &[&tenant_id, &parent_run_id],
            )
            .await
            .map_err(map_database_error)?
            .iter()
            .map(|row| decode_run(context.tenant_id, row))
            .collect()
    }

    /// Lists immutable Plan revisions for one Run in revision order.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or invalid
    /// persisted revision metadata.
    pub async fn list_plan_revisions(
        &self,
        client: &Client,
        context: &QueryContext,
        run_id: RunId,
    ) -> StoreResult<Vec<PlanRevisionSnapshot>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_uuid = uuid(run_id.into_bytes());
        client
            .query(
                "SELECT plan_revision_id, revision, parent_plan_revision_id, schema_version, \
                        plan_key, plan_json, plan_digest, change_summary_json, created_event_id, \
                        created_by, (extract(epoch FROM created_at) * 1000000)::bigint \
                 FROM agent_loom.plan_revisions WHERE tenant_id = $1 AND run_id = $2 \
                 ORDER BY revision, plan_revision_id",
                &[&tenant_id, &run_uuid],
            )
            .await
            .map_err(map_database_error)?
            .iter()
            .map(|row| decode_plan_revision(context.tenant_id, run_id, row))
            .collect()
    }

    /// Lists immutable Context snapshots for one Run in revision order.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or malformed
    /// persisted Context metadata.
    pub async fn list_context_snapshots(
        &self,
        client: &Client,
        context: &QueryContext,
        run_id: RunId,
    ) -> StoreResult<Vec<ContextSnapshot>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_uuid = uuid(run_id.into_bytes());
        client
            .query(
                "SELECT context_snapshot_id, revision, parent_context_snapshot_id, \
                        schema_version, context_json, context_digest, created_event_id, \
                        created_by, (extract(epoch FROM created_at) * 1000000)::bigint \
                 FROM agent_loom.context_snapshots WHERE tenant_id = $1 AND run_id = $2 \
                 ORDER BY revision, context_snapshot_id",
                &[&tenant_id, &run_uuid],
            )
            .await
            .map_err(map_database_error)?
            .iter()
            .map(|row| decode_context_snapshot(context.tenant_id, run_id, row))
            .collect()
    }

    /// Resolves the immutable Context reference and projection bound to a Task.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or malformed
    /// persisted projection data.
    pub async fn get_task_context(
        &self,
        client: &Client,
        context: &QueryContext,
        task_id: TaskId,
    ) -> StoreResult<Option<TaskContextReference>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let task_uuid = uuid(task_id.into_bytes());
        client
            .query_opt(
                "SELECT r.run_id, r.context_snapshot_id, r.projection_json, s.context_json \
                 FROM agent_loom.task_context_references r \
                 JOIN agent_loom.context_snapshots s \
                   ON s.tenant_id = r.tenant_id AND s.run_id = r.run_id \
                  AND s.context_snapshot_id = r.context_snapshot_id \
                 WHERE r.tenant_id = $1 AND r.task_id = $2",
                &[&tenant_id, &task_uuid],
            )
            .await
            .map_err(map_database_error)?
            .map(|row| {
                let projection: Value = row.get(2);
                let source: Value = row.get(3);
                let projected = project_context(&source, &projection)?;
                Ok(TaskContextReference {
                    tenant_id: context.tenant_id,
                    run_id: RunId::from_bytes(row.get::<_, Uuid>(0).into_bytes()),
                    task_id,
                    context_snapshot_id: ContextSnapshotId::from_bytes(
                        row.get::<_, Uuid>(1).into_bytes(),
                    ),
                    projection: decode_json_payload(&projection)?,
                    context: decode_json_payload(&projected)?,
                })
            })
            .transpose()
    }

    /// Reads the latest published/draft version of a Workflow definition.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or invalid persisted data.
    pub async fn get_workflow(
        &self,
        client: &Client,
        context: &QueryContext,
        workflow_id: WorkflowId,
    ) -> StoreResult<Option<WorkflowSnapshot>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let workflow_id = uuid(workflow_id.into_bytes());
        let row = client
            .query_opt(
                "SELECT w.workflow_id, v.workflow_version_id, w.workflow_key, w.name, w.status, \
                        v.version, v.lifecycle, v.spec_json, v.spec_digest, \
                        (extract(epoch FROM v.created_at) * 1000000)::bigint, \
                        (extract(epoch FROM w.updated_at) * 1000000)::bigint \
                 FROM agent_loom.workflow_definitions w \
                 JOIN agent_loom.workflow_definition_versions v \
                   ON v.tenant_id = w.tenant_id AND v.workflow_id = w.workflow_id \
                  AND v.version = w.latest_version \
                 WHERE w.tenant_id = $1 AND w.workflow_id = $2",
                &[&tenant_id, &workflow_id],
            )
            .await
            .map_err(map_database_error)?;
        row.map(|row| decode_workflow(context.tenant_id, &row))
            .transpose()
    }

    /// Lists the business Stage projection for one Run in definition order.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or invalid persisted data.
    pub async fn list_stages(
        &self,
        client: &Client,
        context: &QueryContext,
        run_id: RunId,
    ) -> StoreResult<Vec<StageExecutionSnapshot>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_uuid = uuid(run_id.into_bytes());
        client
            .query(
                "SELECT stage_execution_id, stage_key, definition_stage_key, status, version, \
                        attempt, assignee_kind, assignee_ref, input_contract_json, \
                        output_contract_json, \
                        CASE WHEN started_at IS NULL THEN NULL ELSE \
                            (extract(epoch FROM started_at) * 1000000)::bigint END, \
                        CASE WHEN completed_at IS NULL THEN NULL ELSE \
                            (extract(epoch FROM completed_at) * 1000000)::bigint END, \
                        (extract(epoch FROM created_at) * 1000000)::bigint, \
                        (extract(epoch FROM updated_at) * 1000000)::bigint \
                 FROM agent_loom.stage_executions \
                 WHERE tenant_id = $1 AND run_id = $2 \
                 ORDER BY created_at, stage_key, attempt",
                &[&tenant_id, &run_uuid],
            )
            .await
            .map_err(map_database_error)?
            .iter()
            .map(|row| decode_stage(context.tenant_id, run_id, row))
            .collect()
    }

    /// Lists immutable Artifact references produced by one Run.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or invalid persisted data.
    pub async fn list_artifacts(
        &self,
        client: &Client,
        context: &QueryContext,
        run_id: RunId,
    ) -> StoreResult<Vec<ArtifactRefSnapshot>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_uuid = uuid(run_id.into_bytes());
        client
            .query(
                "SELECT artifact_id, stage_execution_id, task_id, logical_key, kind, \
                        contract_version, version, uri, digest, media_type, size_bytes, \
                        source_artifact_refs_json, metadata_json, produced_by, created_event_id, \
                        (extract(epoch FROM created_at) * 1000000)::bigint \
                 FROM agent_loom.artifact_refs \
                 WHERE tenant_id = $1 AND run_id = $2 \
                 ORDER BY logical_key, version, artifact_id",
                &[&tenant_id, &run_uuid],
            )
            .await
            .map_err(map_database_error)?
            .iter()
            .map(|row| decode_artifact(context.tenant_id, run_id, row))
            .collect()
    }

    /// Lists the durable Wait projection for one Run in creation order.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for unavailable PostgreSQL or invalid data.
    pub async fn list_waits(
        &self,
        client: &Client,
        context: &QueryContext,
        run_id: RunId,
    ) -> StoreResult<Vec<WaitSnapshot>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_uuid = uuid(run_id.into_bytes());
        client
            .query(
                "SELECT wait_id, stage_execution_id, wait_type, expected_event_type, \
                        match_key_hash, status, active_slot, \
                        CASE WHEN expires_at IS NULL THEN NULL ELSE \
                            (extract(epoch FROM expires_at) * 1000000)::bigint END, \
                        consumed_by_event_id, created_event_id \
                 FROM agent_loom.wait_subscriptions \
                 WHERE tenant_id = $1 AND run_id = $2 ORDER BY created_at, wait_id",
                &[&tenant_id, &run_uuid],
            )
            .await
            .map_err(map_database_error)?
            .iter()
            .map(|row| decode_wait(context.tenant_id, run_id, row))
            .collect()
    }

    /// Returns a stable, sequence-ordered Event page scoped to one Run and tenant.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for a zero page size, an unavailable store,
    /// or invalid persisted Event data.
    pub async fn list_events(
        &self,
        client: &Client,
        context: &QueryContext,
        cursor: EventCursor,
    ) -> StoreResult<EventPage> {
        if cursor.limit == 0 {
            return Err(StoreError::new(
                StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "event page limit must be positive",
            ));
        }
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let run_id = uuid(cursor.run_id.into_bytes());
        let after_sequence = i64::try_from(cursor.after_sequence).map_err(|_| {
            StoreError::new(
                StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "event cursor exceeds database range",
            )
        })?;
        let page_size = cursor.limit.min(MAX_EVENT_PAGE_SIZE);
        let fetch_size = i64::from(page_size) + 1;
        let rows = client
            .query(
                "SELECT event_id, sequence, event_type, payload_schema_version, payload_json, \
                        CASE WHEN occurred_at IS NULL THEN NULL \
                             ELSE (extract(epoch FROM occurred_at) * 1000000)::bigint END, \
                        (extract(epoch FROM recorded_at) * 1000000)::bigint \
                 FROM agent_loom.events \
                 WHERE tenant_id = $1 AND run_id = $2 AND sequence > $3 \
                 ORDER BY sequence LIMIT $4",
                &[&tenant_id, &run_id, &after_sequence, &fetch_size],
            )
            .await
            .map_err(map_database_error)?;
        let has_more = rows.len() > page_size as usize;
        let events = rows
            .iter()
            .take(page_size as usize)
            .map(|row| decode_event(context.tenant_id, cursor.run_id, row))
            .collect::<StoreResult<Vec<_>>>()?;
        let next_after_sequence = has_more
            .then(|| events.last().map(|event| event.sequence))
            .flatten();
        Ok(EventPage {
            events,
            next_after_sequence,
        })
    }

    /// Scans currently due Tool/Agent retry candidates using database time and
    /// a stable `(due_at, kind, execution_id)` keyset. Candidates are hints;
    /// the apply transaction must lock and revalidate the target revision.
    ///
    /// # Errors
    ///
    /// Returns a stable store error for an invalid limit, cursor conversion,
    /// unavailable PostgreSQL, or an invalid persisted candidate.
    pub async fn scan_due_work(
        &self,
        client: &Client,
        context: &QueryContext,
        query: DueWorkQuery,
    ) -> StoreResult<DueWorkPage> {
        if query.limit == 0 {
            return Err(StoreError::new(
                StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "due-work page limit must be positive",
            ));
        }
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let after_due = query.after.map(|cursor| cursor.due_at.get());
        let after_kind = query.after.map(|cursor| due_work_kind(cursor.kind));
        let after_id = query
            .after
            .map(|cursor| Uuid::from_bytes(cursor.execution_id));
        let page_size = query.limit.min(MAX_DUE_WORK_PAGE_SIZE);
        let fetch_size = i64::from(page_size) + 1;
        let rows = client
            .query(
                "SELECT kind, execution_id, run_id, stage_execution_id, due_micros, expected_revision, \
                        run_version, execution_generation, checkpoint_sequence FROM (\
                    SELECT 'tool_retry'::text AS kind, x.tool_execution_id AS execution_id, \
                           x.run_id, x.stage_execution_id, \
                           (extract(epoch FROM x.retry_at) * 1000000)::bigint AS due_micros, \
                           x.attempt_count AS expected_revision, r.version AS run_version, \
                           r.execution_generation, c.sequence AS checkpoint_sequence \
                    FROM agent_loom.tool_executions x \
                    JOIN agent_loom.runs r ON r.tenant_id = x.tenant_id AND r.run_id = x.run_id \
                    JOIN agent_loom.checkpoints c ON c.tenant_id = r.tenant_id \
                      AND c.run_id = r.run_id AND c.checkpoint_id = r.current_checkpoint_id \
                    WHERE x.tenant_id = $1 AND x.status = 'retry_scheduled' \
                      AND x.retry_at <= transaction_timestamp() \
                      AND r.status IN ('queued', 'running', 'waiting', 'approval_required', \
                                       'retrying', 'paused') \
                    UNION ALL \
                    SELECT 'agent_retry'::text AS kind, x.agent_execution_id AS execution_id, \
                           x.run_id, x.stage_execution_id, \
                           (extract(epoch FROM x.retry_at) * 1000000)::bigint AS due_micros, \
                           x.version AS expected_revision, r.version AS run_version, \
                           r.execution_generation, c.sequence AS checkpoint_sequence \
                    FROM agent_loom.agent_executions x \
                    JOIN agent_loom.runs r ON r.tenant_id = x.tenant_id AND r.run_id = x.run_id \
                    JOIN agent_loom.checkpoints c ON c.tenant_id = r.tenant_id \
                      AND c.run_id = r.run_id AND c.checkpoint_id = r.current_checkpoint_id \
                    WHERE x.tenant_id = $1 AND x.status = 'reconciling' \
                      AND x.retry_at IS NOT NULL AND x.retry_at <= transaction_timestamp() \
                      AND r.status IN ('queued', 'running', 'waiting', 'approval_required', \
                                       'retrying', 'paused')\
                 ) due \
                 WHERE $2::bigint IS NULL OR (due_micros, kind, execution_id) > \
                       ($2::bigint, $3::text, $4::uuid) \
                 ORDER BY due_micros, kind, execution_id LIMIT $5",
                &[&tenant_id, &after_due, &after_kind, &after_id, &fetch_size],
            )
            .await
            .map_err(map_database_error)?;
        let has_more = rows.len() > page_size as usize;
        let candidates = rows
            .iter()
            .take(page_size as usize)
            .map(|row| decode_due_work(context.tenant_id, row))
            .collect::<StoreResult<Vec<_>>>()?;
        let next_cursor = has_more
            .then(|| candidates.last().copied().map(DueWorkCandidate::cursor))
            .flatten();
        Ok(DueWorkPage {
            candidates,
            next_cursor,
        })
    }

    /// Scans Run deadlines, Wait expirations, and stale external executions
    /// against PostgreSQL transaction time using one stable keyset.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for invalid bounds, database failures, or
    /// invalid persisted candidate data.
    pub async fn scan_maintenance(
        &self,
        client: &Client,
        context: &QueryContext,
        query: MaintenanceQuery,
    ) -> StoreResult<MaintenancePage> {
        if query.limit == 0 || query.stale_after_micros == 0 {
            return Err(StoreError::new(
                StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "maintenance limit and stale interval must be positive",
            ));
        }
        let stale_after = i64::try_from(query.stale_after_micros).map_err(|_| {
            StoreError::new(
                StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "maintenance stale interval exceeds PostgreSQL range",
            )
        })?;
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let after_due = query.after.map(|cursor| cursor.due_at.get());
        let after_kind = query.after.map(|cursor| maintenance_kind(cursor.kind));
        let after_id = query.after.map(|cursor| Uuid::from_bytes(cursor.target_id));
        let page_size = query.limit.min(MAX_MAINTENANCE_PAGE_SIZE);
        let fetch_size = i64::from(page_size) + 1;
        let rows = client
            .query(
                "SELECT kind, target_id, run_id, due_micros, expected_revision, run_version, \
                        execution_generation FROM (\
                    SELECT 'run_deadline'::text AS kind, r.run_id AS target_id, r.run_id, \
                           (extract(epoch FROM r.deadline) * 1000000)::bigint AS due_micros, \
                           r.version AS expected_revision, r.version AS run_version, \
                           r.execution_generation \
                    FROM agent_loom.runs r \
                    WHERE r.tenant_id = $1 AND r.deadline <= transaction_timestamp() \
                      AND r.status IN ('queued', 'running', 'waiting', 'approval_required', \
                                       'retrying', 'paused') \
                    UNION ALL \
                    SELECT 'wait_timeout'::text, w.wait_id, w.run_id, \
                           (extract(epoch FROM w.expires_at) * 1000000)::bigint, \
                           0::bigint, r.version, r.execution_generation \
                    FROM agent_loom.wait_subscriptions w \
                    JOIN agent_loom.runs r ON r.tenant_id = w.tenant_id AND r.run_id = w.run_id \
                    WHERE w.tenant_id = $1 AND w.status = 'open' \
                      AND w.expires_at <= transaction_timestamp() \
                      AND r.status IN ('waiting', 'approval_required', 'paused') \
                    UNION ALL \
                    SELECT 'tool_stale'::text, x.tool_execution_id, x.run_id, \
                           (extract(epoch FROM x.updated_at) * 1000000)::bigint + $5::bigint, \
                           x.attempt_count, r.version, r.execution_generation \
                    FROM agent_loom.tool_executions x \
                    JOIN agent_loom.runs r ON r.tenant_id = x.tenant_id AND r.run_id = x.run_id \
                    WHERE x.tenant_id = $1 AND x.status IN ('executing', 'outcome_unknown') \
                      AND x.updated_at <= transaction_timestamp() - ($5::bigint * interval '1 microsecond') \
                      AND r.status IN ('queued', 'running', 'waiting', 'approval_required', \
                                       'retrying', 'paused') \
                    UNION ALL \
                    SELECT 'agent_stale'::text, x.agent_execution_id, x.run_id, \
                           (extract(epoch FROM x.updated_at) * 1000000)::bigint + $5::bigint, \
                           x.version, r.version, r.execution_generation \
                    FROM agent_loom.agent_executions x \
                    JOIN agent_loom.runs r ON r.tenant_id = x.tenant_id AND r.run_id = x.run_id \
                    WHERE x.tenant_id = $1 \
                      AND x.status IN ('submitting', 'running', 'outcome_unknown') \
                      AND x.updated_at <= transaction_timestamp() - ($5::bigint * interval '1 microsecond') \
                      AND r.status IN ('queued', 'running', 'waiting', 'approval_required', \
                                       'retrying', 'paused')\
                 ) due \
                 WHERE $2::bigint IS NULL OR (due_micros, kind, target_id) > \
                       ($2::bigint, $3::text, $4::uuid) \
                 ORDER BY due_micros, kind, target_id LIMIT $6",
                &[&tenant_id, &after_due, &after_kind, &after_id, &stale_after, &fetch_size],
            )
            .await
            .map_err(map_database_error)?;
        let has_more = rows.len() > page_size as usize;
        let candidates = rows
            .iter()
            .take(page_size as usize)
            .map(|row| decode_maintenance(context.tenant_id, row))
            .collect::<StoreResult<Vec<_>>>()?;
        let next_cursor = has_more
            .then(|| candidates.last().copied().map(MaintenanceCandidate::cursor))
            .flatten();
        Ok(MaintenancePage {
            candidates,
            next_cursor,
        })
    }

    /// Returns bounded remote-stop work backed by authoritative Agent and Run revisions.
    ///
    /// Rows remain visible until a stop outcome commits. Multiple processes may
    /// observe the same row; the runtime therefore uses a stable idempotency key
    /// and the outcome transaction fences on the Agent execution version.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for an invalid limit, database failure, or
    /// malformed persisted Agent state.
    pub async fn scan_agent_stops(
        &self,
        client: &Client,
        context: &QueryContext,
        query: AgentStopQuery,
    ) -> StoreResult<AgentStopPage> {
        if query.limit == 0 {
            return Err(StoreError::new(
                StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "agent stop page limit must be positive",
            ));
        }
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let limit = i64::from(query.limit.min(MAX_AGENT_STOP_PAGE_SIZE));
        let rows = client
            .query(
                "SELECT x.agent_execution_id, x.run_id, x.stage_execution_id, x.task_id, \
                        x.endpoint_id, x.agent_version_id, x.status, x.version, \
                        x.remote_run_ref, x.remote_session_ref, x.remote_protocol_version, \
                        x.event_cursor, x.cursor_version, \
                        CASE WHEN x.retry_at IS NULL THEN NULL ELSE \
                            (extract(epoch FROM x.retry_at) * 1000000)::bigint END, \
                        (extract(epoch FROM x.updated_at) * 1000000)::bigint, \
                        r.version, r.execution_generation \
                 FROM agent_loom.agent_executions x \
                 JOIN agent_loom.runs r ON r.tenant_id = x.tenant_id AND r.run_id = x.run_id \
                 WHERE x.tenant_id = $1 AND x.status = 'stopping' \
                   AND x.remote_run_ref IS NOT NULL \
                 ORDER BY x.updated_at, x.agent_execution_id LIMIT $2",
                &[&tenant_id, &limit],
            )
            .await
            .map_err(map_database_error)?;
        let candidates = rows
            .iter()
            .map(|row| decode_agent_stop(context.tenant_id, row))
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(AgentStopPage { candidates })
    }

    /// Returns due remote status polls using PostgreSQL transaction time.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for an invalid limit, database failure, or
    /// malformed persisted Agent state.
    pub async fn scan_agent_status(
        &self,
        client: &Client,
        context: &QueryContext,
        query: AgentStatusQuery,
    ) -> StoreResult<AgentStatusPage> {
        if query.limit == 0 {
            return Err(StoreError::new(
                StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "agent status page limit must be positive",
            ));
        }
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let limit = i64::from(query.limit.min(MAX_AGENT_STATUS_PAGE_SIZE));
        let rows = client
            .query(
                "SELECT x.agent_execution_id, x.run_id, x.stage_execution_id, x.task_id, \
                        x.endpoint_id, x.agent_version_id, x.status, x.version, \
                        x.remote_run_ref, x.remote_session_ref, x.remote_protocol_version, \
                        x.event_cursor, x.cursor_version, \
                        CASE WHEN x.retry_at IS NULL THEN NULL ELSE \
                            (extract(epoch FROM x.retry_at) * 1000000)::bigint END, \
                        (extract(epoch FROM x.updated_at) * 1000000)::bigint, \
                        r.version, r.execution_generation, \
                        (extract(epoch FROM x.status_poll_at) * 1000000)::bigint \
                 FROM agent_loom.agent_executions x \
                 JOIN agent_loom.runs r ON r.tenant_id = x.tenant_id AND r.run_id = x.run_id \
                 WHERE x.tenant_id = $1 AND x.status = 'reconciling' \
                   AND x.remote_run_ref IS NOT NULL \
                   AND x.remote_protocol_version IS NOT NULL \
                   AND x.status_poll_at <= transaction_timestamp() \
                 ORDER BY x.status_poll_at, x.agent_execution_id LIMIT $2",
                &[&tenant_id, &limit],
            )
            .await
            .map_err(map_database_error)?;
        let candidates = rows
            .iter()
            .map(|row| decode_agent_status(context.tenant_id, row))
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(AgentStatusPage { candidates })
    }

    /// Returns due resumable remote event reads using PostgreSQL transaction time.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error for an invalid limit, database failure, or
    /// malformed persisted Agent state.
    pub async fn scan_agent_events(
        &self,
        client: &Client,
        context: &QueryContext,
        query: AgentEventQuery,
    ) -> StoreResult<AgentEventPage> {
        if query.limit == 0 {
            return Err(StoreError::new(
                StoreErrorCode::ConstraintViolation,
                agent_loom_durable_store::RetryClass::Never,
                "agent event page limit must be positive",
            ));
        }
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let limit = i64::from(query.limit.min(MAX_AGENT_EVENT_PAGE_SIZE));
        let rows = client
            .query(
                "SELECT x.agent_execution_id, x.run_id, x.stage_execution_id, x.task_id, \
                        x.endpoint_id, x.agent_version_id, x.status, x.version, \
                        x.remote_run_ref, x.remote_session_ref, x.remote_protocol_version, \
                        x.event_cursor, x.cursor_version, \
                        CASE WHEN x.retry_at IS NULL THEN NULL ELSE \
                            (extract(epoch FROM x.retry_at) * 1000000)::bigint END, \
                        (extract(epoch FROM x.updated_at) * 1000000)::bigint, \
                        r.version, r.execution_generation, \
                        (extract(epoch FROM x.status_poll_at) * 1000000)::bigint \
                 FROM agent_loom.agent_executions x \
                 JOIN agent_loom.runs r ON r.tenant_id = x.tenant_id AND r.run_id = x.run_id \
                 WHERE x.tenant_id = $1 AND x.status = 'running' \
                   AND x.remote_run_ref IS NOT NULL \
                   AND x.remote_protocol_version IS NOT NULL \
                   AND x.status_poll_at <= transaction_timestamp() \
                 ORDER BY x.status_poll_at, x.agent_execution_id LIMIT $2",
                &[&tenant_id, &limit],
            )
            .await
            .map_err(map_database_error)?;
        let candidates = rows
            .iter()
            .map(|row| decode_agent_event(context.tenant_id, row))
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(AgentEventPage { candidates })
    }

    /// Loads the immutable Tool Adapter invocation envelope for one execution.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error when PostgreSQL is unavailable or persisted
    /// identity/request data violates the portable contract.
    pub async fn get_tool_invocation(
        &self,
        client: &Client,
        context: &QueryContext,
        execution_id: ToolExecutionId,
    ) -> StoreResult<Option<ToolInvocation>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let execution_id = uuid(execution_id.into_bytes());
        let row = client
            .query_opt(
                "SELECT run_id, tool_name, idempotency_scope, idempotency_key, \
                        request_hash, request_json \
                 FROM agent_loom.tool_executions \
                 WHERE tenant_id = $1 AND tool_execution_id = $2",
                &[&tenant_id, &execution_id],
            )
            .await
            .map_err(map_database_error)?;
        row.map(|row| {
            Ok(ToolInvocation {
                tenant_id: context.tenant_id,
                tool_execution_id: ToolExecutionId::from_bytes(execution_id.into_bytes()),
                run_id: run_id_from_uuid(row.get(0)),
                tool_name: row.get(1),
                idempotency_scope: ScopeKey::parse(row.get::<_, String>(2))
                    .map_err(|_| inconsistent("Tool invocation has an invalid scope"))?,
                idempotency_key: IdempotencyKey::parse(row.get::<_, String>(3))
                    .map_err(|_| inconsistent("Tool invocation has an invalid idempotency key"))?,
                request_hash: decode_digest(row.get(4), "Tool invocation request hash")?,
                request: decode_json_payload(&row.get(5))?,
            })
        })
        .transpose()
    }

    /// Loads the immutable Agent Server invocation envelope for one execution.
    ///
    /// # Errors
    ///
    /// Returns a stable Store error when PostgreSQL is unavailable or persisted
    /// identity/request data violates the portable contract.
    pub async fn get_agent_invocation(
        &self,
        client: &Client,
        context: &QueryContext,
        execution_id: AgentExecutionId,
    ) -> StoreResult<Option<AgentInvocation>> {
        let tenant_id = uuid(context.tenant_id.into_bytes());
        let execution_id = uuid(execution_id.into_bytes());
        let row = client
            .query_opt(
                "SELECT run_id, endpoint_id, agent_version_id, idempotency_key, \
                        request_hash, request_json, capabilities_snapshot_json \
                 FROM agent_loom.agent_executions \
                 WHERE tenant_id = $1 AND agent_execution_id = $2",
                &[&tenant_id, &execution_id],
            )
            .await
            .map_err(map_database_error)?;
        row.map(|row| {
            let endpoint_id: Uuid = row.get(1);
            let agent_version_id: Uuid = row.get(2);
            Ok(AgentInvocation {
                tenant_id: context.tenant_id,
                agent_execution_id: AgentExecutionId::from_bytes(execution_id.into_bytes()),
                run_id: run_id_from_uuid(row.get(0)),
                endpoint_id: EndpointId::from_bytes(endpoint_id.into_bytes()),
                agent_version_id: AgentVersionId::from_bytes(agent_version_id.into_bytes()),
                idempotency_key: IdempotencyKey::parse(row.get::<_, String>(3))
                    .map_err(|_| inconsistent("Agent invocation has an invalid idempotency key"))?,
                request_hash: decode_digest(row.get(4), "Agent invocation request hash")?,
                request: decode_json_payload(&row.get(5))?,
                capabilities_snapshot: decode_json_payload(&row.get(6))?,
            })
        })
        .transpose()
    }
}

fn decode_digest(value: Vec<u8>, field: &str) -> StoreResult<Digest> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|_| inconsistent(&format!("{field} has an invalid length")))?;
    Ok(Digest::from_bytes(bytes))
}

fn decode_plan_revision(
    tenant_id: TenantId,
    run_id: RunId,
    row: &Row,
) -> StoreResult<PlanRevisionSnapshot> {
    let schema_version = u32::try_from(row.get::<_, i64>(3))
        .map_err(|_| inconsistent("Plan schema version exceeds u32 range"))?;
    if schema_version == 0 {
        return Err(inconsistent("Plan schema version is zero"));
    }
    Ok(PlanRevisionSnapshot {
        tenant_id,
        plan_revision_id: PlanRevisionId::from_bytes(row.get::<_, Uuid>(0).into_bytes()),
        run_id,
        revision: nonnegative_u64(row.get(1), "Plan revision")?,
        parent_plan_revision_id: row
            .get::<_, Option<Uuid>>(2)
            .map(|id| PlanRevisionId::from_bytes(id.into_bytes())),
        schema_version,
        plan_key: LogicalKey::parse(row.get::<_, String>(4))
            .map_err(|_| inconsistent("Plan revision has an invalid key"))?,
        plan: decode_json_payload(&row.get(5))?,
        plan_digest: decode_digest(row.get(6), "Plan digest")?,
        change_summary: decode_json_payload(&row.get(7))?,
        created_event_id: event_id_from_uuid(row.get(8)),
        created_by: row.get(9),
        created_at: UnixMicros::new(row.get(10)),
    })
}

fn decode_context_snapshot(
    tenant_id: TenantId,
    run_id: RunId,
    row: &Row,
) -> StoreResult<ContextSnapshot> {
    let revision: i64 = row.get(1);
    let schema_version = u32::try_from(row.get::<_, i32>(3))
        .map_err(|_| inconsistent("Context schema version exceeds u32 range"))?;
    if schema_version == 0 {
        return Err(inconsistent("Context schema version is zero"));
    }
    let digest: Vec<u8> = row.get(5);
    Ok(ContextSnapshot {
        tenant_id,
        context_snapshot_id: ContextSnapshotId::from_bytes(row.get::<_, Uuid>(0).into_bytes()),
        run_id,
        revision: nonnegative_u64(revision, "Context revision")?,
        parent_context_snapshot_id: row
            .get::<_, Option<Uuid>>(2)
            .map(|id| ContextSnapshotId::from_bytes(id.into_bytes())),
        schema_version,
        value: decode_json_payload(&row.get(4))?,
        digest: decode_digest(digest, "Context digest")?,
        created_event_id: EventId::from_bytes(row.get::<_, Uuid>(6).into_bytes()),
        created_by: row.get(7),
        created_at: UnixMicros::new(row.get(8)),
    })
}

fn decode_agent_stop(tenant_id: TenantId, row: &Row) -> StoreResult<AgentStopCandidate> {
    let (execution, expected_run) = decode_agent_control_execution(tenant_id, row)?;
    let candidate = AgentStopCandidate {
        tenant_id,
        execution,
        expected_run,
    };
    if !candidate.shape_is_valid() {
        return Err(inconsistent(
            "Agent stop candidate violates its durable shape",
        ));
    }
    Ok(candidate)
}

fn decode_agent_control_execution(
    tenant_id: TenantId,
    row: &Row,
) -> StoreResult<(
    AgentExecutionSnapshot,
    agent_loom_durable_store::ExpectedRun,
)> {
    let execution_id: Uuid = row.get(0);
    let run_uuid: Uuid = row.get(1);
    let stage_id: Option<Uuid> = row.get(2);
    let task_id: Uuid = row.get(3);
    let endpoint_id: Uuid = row.get(4);
    let agent_version_id: Uuid = row.get(5);
    let run_id = run_id_from_uuid(run_uuid);
    Ok((
        AgentExecutionSnapshot {
            tenant_id,
            agent_execution_id: AgentExecutionId::from_bytes(execution_id.into_bytes()),
            run_id,
            stage_execution_id: stage_id
                .map(|value| StageExecutionId::from_bytes(value.into_bytes())),
            task_id: TaskId::from_bytes(task_id.into_bytes()),
            endpoint_id: EndpointId::from_bytes(endpoint_id.into_bytes()),
            agent_version_id: AgentVersionId::from_bytes(agent_version_id.into_bytes()),
            status: crate::parse_agent_status(row.get::<_, &str>(6))?,
            version: nonnegative_u64(row.get(7), "Agent execution version")?,
            remote_run_ref: row.get(8),
            remote_session_ref: row.get(9),
            remote_protocol_version: row.get(10),
            status_poll_at: None,
            event_cursor: row.get(11),
            cursor_version: nonnegative_u64(row.get(12), "Agent cursor version")?,
            retry_at: row.get::<_, Option<i64>>(13).map(UnixMicros::new),
            updated_at: UnixMicros::new(row.get(14)),
        },
        agent_loom_durable_store::ExpectedRun {
            run_id,
            version: Some(nonnegative_u64(row.get(15), "Run version")?),
            execution_generation: Some(nonnegative_u64(row.get(16), "Run execution generation")?),
        },
    ))
}

fn decode_agent_status(tenant_id: TenantId, row: &Row) -> StoreResult<AgentStatusCandidate> {
    let (mut execution, expected_run) = decode_agent_control_execution(tenant_id, row)?;
    execution.status_poll_at = Some(UnixMicros::new(row.get(17)));
    let candidate = AgentStatusCandidate {
        tenant_id,
        execution,
        expected_run,
    };
    if !candidate.shape_is_valid() {
        return Err(inconsistent(
            "Agent status candidate violates its durable shape",
        ));
    }
    Ok(candidate)
}

fn decode_agent_event(tenant_id: TenantId, row: &Row) -> StoreResult<AgentEventCandidate> {
    let (mut execution, expected_run) = decode_agent_control_execution(tenant_id, row)?;
    execution.status_poll_at = Some(UnixMicros::new(row.get(17)));
    let candidate = AgentEventCandidate {
        tenant_id,
        execution,
        expected_run,
    };
    if !candidate.shape_is_valid() {
        return Err(inconsistent(
            "Agent event candidate violates its durable shape",
        ));
    }
    Ok(candidate)
}

fn decode_workflow(tenant_id: TenantId, row: &Row) -> StoreResult<WorkflowSnapshot> {
    let workflow_id: Uuid = row.get(0);
    let workflow_version_id: Uuid = row.get(1);
    Ok(WorkflowSnapshot {
        tenant_id,
        workflow_id: WorkflowId::from_bytes(workflow_id.into_bytes()),
        workflow_version_id: WorkflowVersionId::from_bytes(workflow_version_id.into_bytes()),
        workflow_key: row.get(2),
        name: row.get(3),
        status: row.get(4),
        version: nonnegative_u64(row.get(5), "workflow version")?,
        lifecycle: row.get(6),
        spec: decode_json_payload(&row.get(7))?,
        spec_digest: decode_digest(row.get(8), "workflow spec digest")?,
        created_at: UnixMicros::new(row.get(9)),
        updated_at: UnixMicros::new(row.get(10)),
    })
}

fn decode_stage(
    tenant_id: TenantId,
    run_id: RunId,
    row: &Row,
) -> StoreResult<StageExecutionSnapshot> {
    let stage_id: Uuid = row.get(0);
    let attempt = u32::try_from(row.get::<_, i64>(5))
        .map_err(|_| inconsistent("stage attempt is outside u32 range"))?;
    Ok(StageExecutionSnapshot {
        tenant_id,
        stage_execution_id: StageExecutionId::from_bytes(stage_id.into_bytes()),
        run_id,
        stage_key: LogicalKey::parse(row.get::<_, String>(1))
            .map_err(|_| inconsistent("stage key is invalid"))?,
        definition_stage_key: row
            .get::<_, Option<String>>(2)
            .map(LogicalKey::parse)
            .transpose()
            .map_err(|_| inconsistent("definition stage key is invalid"))?,
        status: parse_stage_status(row.get(3))?,
        version: nonnegative_u64(row.get(4), "stage version")?,
        attempt,
        assignee_kind: row.get(6),
        assignee_ref: row.get(7),
        input_contract: decode_json_payload(&row.get(8))?,
        output_contract: decode_json_payload(&row.get(9))?,
        started_at: row.get::<_, Option<i64>>(10).map(UnixMicros::new),
        completed_at: row.get::<_, Option<i64>>(11).map(UnixMicros::new),
        created_at: UnixMicros::new(row.get(12)),
        updated_at: UnixMicros::new(row.get(13)),
    })
}

fn decode_artifact(
    tenant_id: TenantId,
    run_id: RunId,
    row: &Row,
) -> StoreResult<ArtifactRefSnapshot> {
    let artifact_id: Uuid = row.get(0);
    let sources = row
        .get::<_, Value>(11)
        .as_array()
        .ok_or_else(|| inconsistent("artifact source list is not an array"))?
        .iter()
        .map(|source| {
            let id = source
                .get("artifact_id")
                .and_then(Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok())
                .ok_or_else(|| inconsistent("artifact source ID is invalid"))?;
            let version = source
                .get("version")
                .and_then(Value::as_u64)
                .ok_or_else(|| inconsistent("artifact source version is invalid"))?;
            Ok(ArtifactVersionRef {
                artifact_id: ArtifactId::from_bytes(id.into_bytes()),
                version,
            })
        })
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(ArtifactRefSnapshot {
        tenant_id,
        artifact_id: ArtifactId::from_bytes(artifact_id.into_bytes()),
        run_id,
        stage_execution_id: row
            .get::<_, Option<Uuid>>(1)
            .map(|id| StageExecutionId::from_bytes(id.into_bytes())),
        task_id: row
            .get::<_, Option<Uuid>>(2)
            .map(|id| agent_loom_domain::TaskId::from_bytes(id.into_bytes())),
        logical_key: LogicalKey::parse(row.get::<_, String>(3))
            .map_err(|_| inconsistent("artifact logical key is invalid"))?,
        kind: row.get(4),
        contract_version: u32::try_from(row.get::<_, i64>(5))
            .map_err(|_| inconsistent("artifact contract version is invalid"))?,
        version: nonnegative_u64(row.get(6), "artifact version")?,
        uri: row.get(7),
        digest: decode_digest(row.get(8), "artifact digest")?,
        media_type: row.get(9),
        size_bytes: nonnegative_u64(row.get(10), "artifact size")?,
        sources,
        metadata: decode_json_payload(&row.get(12))?,
        produced_by: row.get(13),
        created_event_id: {
            let id: Uuid = row.get(14);
            EventId::from_bytes(id.into_bytes())
        },
        created_at: UnixMicros::new(row.get(15)),
    })
}

fn decode_wait(tenant_id: TenantId, run_id: RunId, row: &Row) -> StoreResult<WaitSnapshot> {
    let wait_id: Uuid = row.get(0);
    let active_slot = row
        .get::<_, Option<i16>>(6)
        .map(u8::try_from)
        .transpose()
        .map_err(|_| inconsistent("Wait active slot is outside u8 range"))?;
    let status = match row.get::<_, &str>(5) {
        "open" => WaitStatus::Open,
        "consumed" => WaitStatus::Consumed,
        "expired" => WaitStatus::Expired,
        "cancelled" => WaitStatus::Cancelled,
        _ => return Err(inconsistent("database returned an unknown Wait status")),
    };
    let snapshot = WaitSnapshot {
        tenant_id,
        wait_id: WaitId::from_bytes(wait_id.into_bytes()),
        run_id,
        stage_execution_id: row
            .get::<_, Option<Uuid>>(1)
            .map(|id| StageExecutionId::from_bytes(id.into_bytes())),
        wait_type: row.get(2),
        expected_event_type: row.get(3),
        match_key_hash: decode_digest(row.get(4), "Wait match key hash")?,
        status,
        active_slot,
        expires_at: row.get::<_, Option<i64>>(7).map(UnixMicros::new),
        consumed_by_event_id: row.get::<_, Option<Uuid>>(8).map(event_id_from_uuid),
        created_event_id: event_id_from_uuid(row.get(9)),
    };
    if !snapshot.active_slot_invariant_holds() {
        return Err(inconsistent("Wait active slot invariant is violated"));
    }
    Ok(snapshot)
}

fn parse_stage_status(value: &str) -> StoreResult<StageStatus> {
    match value {
        "planned" => Ok(StageStatus::Planned),
        "active" => Ok(StageStatus::Active),
        "waiting_approval" => Ok(StageStatus::WaitingApproval),
        "rework_required" => Ok(StageStatus::ReworkRequired),
        "succeeded" => Ok(StageStatus::Succeeded),
        "failed" => Ok(StageStatus::Failed),
        "skipped" => Ok(StageStatus::Skipped),
        "cancelled" => Ok(StageStatus::Cancelled),
        _ => Err(inconsistent("database returned an unknown stage status")),
    }
}

fn decode_json_payload(value: &Value) -> StoreResult<JsonPayload> {
    serde_json::to_vec(value)
        .map(JsonPayload::from_validated_bytes)
        .map_err(|_| inconsistent("failed to encode persisted invocation JSON"))
}

fn project_context(source: &Value, projection: &Value) -> StoreResult<Value> {
    let pointers = projection
        .as_array()
        .ok_or_else(|| inconsistent("Task ContextProjection is not an array"))?;
    if pointers.is_empty() {
        return Ok(source.clone());
    }
    let mut projected = serde_json::Map::new();
    for pointer in pointers {
        let pointer = pointer
            .as_str()
            .ok_or_else(|| inconsistent("Task ContextProjection entry is not a string"))?;
        let value = source
            .pointer(pointer)
            .ok_or_else(|| inconsistent("Task ContextProjection pointer does not resolve"))?;
        projected.insert(pointer.to_owned(), value.clone());
    }
    Ok(Value::Object(projected))
}

const fn due_work_kind(kind: DueWorkKind) -> &'static str {
    match kind {
        DueWorkKind::ToolRetry => "tool_retry",
        DueWorkKind::AgentRetry => "agent_retry",
    }
}

fn decode_due_work(tenant_id: TenantId, row: &Row) -> StoreResult<DueWorkCandidate> {
    let kind: &str = row.get(0);
    let execution_id: Uuid = row.get(1);
    let target = match kind {
        "tool_retry" => DueWorkTarget::Tool(ToolExecutionId::from_bytes(execution_id.into_bytes())),
        "agent_retry" => {
            DueWorkTarget::Agent(AgentExecutionId::from_bytes(execution_id.into_bytes()))
        }
        _ => return Err(inconsistent("database returned an unknown due-work kind")),
    };
    let checkpoint_sequence = nonnegative_u64(row.get(8), "due-work checkpoint sequence")?;
    if checkpoint_sequence == 0 {
        return Err(inconsistent(
            "database due-work candidate has no checkpoint sequence",
        ));
    }
    Ok(DueWorkCandidate {
        tenant_id,
        run_id: run_id_from_uuid(row.get(2)),
        stage_execution_id: row
            .get::<_, Option<Uuid>>(3)
            .map(|id| agent_loom_domain::StageExecutionId::from_bytes(id.into_bytes())),
        target,
        due_at: UnixMicros::new(row.get(4)),
        expected_revision: nonnegative_u64(row.get(5), "due-work revision")?,
        run_version: nonnegative_u64(row.get(6), "due-work Run version")?,
        execution_generation: nonnegative_u64(row.get(7), "due-work generation")?,
        checkpoint_sequence,
    })
}

const fn maintenance_kind(kind: MaintenanceKind) -> &'static str {
    match kind {
        MaintenanceKind::RunDeadline => "run_deadline",
        MaintenanceKind::WaitTimeout => "wait_timeout",
        MaintenanceKind::ToolStale => "tool_stale",
        MaintenanceKind::AgentStale => "agent_stale",
    }
}

fn decode_maintenance(tenant_id: TenantId, row: &Row) -> StoreResult<MaintenanceCandidate> {
    let kind: &str = row.get(0);
    let target_id: Uuid = row.get(1);
    let target = match kind {
        "run_deadline" => MaintenanceTarget::Run(RunId::from_bytes(target_id.into_bytes())),
        "wait_timeout" => MaintenanceTarget::Wait(WaitId::from_bytes(target_id.into_bytes())),
        "tool_stale" => {
            MaintenanceTarget::Tool(ToolExecutionId::from_bytes(target_id.into_bytes()))
        }
        "agent_stale" => {
            MaintenanceTarget::Agent(AgentExecutionId::from_bytes(target_id.into_bytes()))
        }
        _ => {
            return Err(inconsistent(
                "database returned an unknown maintenance kind",
            ));
        }
    };
    Ok(MaintenanceCandidate {
        tenant_id,
        run_id: run_id_from_uuid(row.get(2)),
        target,
        due_at: UnixMicros::new(row.get(3)),
        expected_revision: nonnegative_u64(row.get(4), "maintenance revision")?,
        run_version: nonnegative_u64(row.get(5), "maintenance Run version")?,
        execution_generation: nonnegative_u64(row.get(6), "maintenance generation")?,
    })
}

fn decode_run(tenant_id: TenantId, row: &Row) -> StoreResult<RunSnapshot> {
    let suspended_from_status = row
        .get::<_, Option<&str>>(3)
        .map(parse_run_status)
        .transpose()?;
    Ok(RunSnapshot {
        tenant_id,
        run_id: run_id_from_uuid(row.get(0)),
        workflow_version_id: row.get::<_, Option<Uuid>>(1).map(workflow_id_from_uuid),
        status: parse_run_status(row.get(2))?,
        suspended_from_status,
        version: nonnegative_u64(row.get(4), "run version")?,
        execution_generation: nonnegative_u64(row.get(5), "execution generation")?,
        next_event_sequence: nonnegative_u64(row.get(6), "event sequence")?,
        current_checkpoint_id: row
            .get::<_, Option<Uuid>>(7)
            .map(|value| CheckpointId::from_bytes(value.into_bytes())),
        terminal_event_id: row.get::<_, Option<Uuid>>(8).map(event_id_from_uuid),
        deadline: row.get::<_, Option<i64>>(9).map(UnixMicros::new),
        updated_at: UnixMicros::new(row.get(10)),
    })
}

fn decode_event(tenant_id: TenantId, run_id: RunId, row: &Row) -> StoreResult<EventRecord> {
    let sequence = nonnegative_u64(row.get(1), "event sequence")?;
    if sequence == 0 {
        return Err(inconsistent("database event sequence is zero"));
    }
    let payload_schema_version = u32::try_from(row.get::<_, i64>(3))
        .map_err(|_| inconsistent("database event schema version is outside u32 range"))?;
    if payload_schema_version == 0 {
        return Err(inconsistent("database event schema version is zero"));
    }
    let payload: Value = row.get(4);
    let payload = serde_json::to_vec(&payload)
        .map(JsonPayload::from_validated_bytes)
        .map_err(|_| inconsistent("database event payload cannot be encoded"))?;
    Ok(EventRecord {
        tenant_id,
        event_id: event_id_from_uuid(row.get(0)),
        run_id,
        sequence,
        event_type: row.get(2),
        payload_schema_version,
        payload,
        occurred_at: row.get::<_, Option<i64>>(5).map(UnixMicros::new),
        recorded_at: UnixMicros::new(row.get(6)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_sql_is_tenant_scoped_and_sequence_ordered() {
        let source = include_str!("query_executor.rs");
        assert!(source.contains("WHERE tenant_id = $1 AND run_id = $2"));
        assert!(source.contains("AND sequence > $3"));
        assert!(source.contains("ORDER BY sequence LIMIT $4"));
        assert_eq!(MAX_EVENT_PAGE_SIZE, 1_000);
    }

    #[test]
    fn due_work_sql_uses_database_time_and_stable_keyset_order() {
        let source = include_str!("query_executor.rs");
        assert!(source.contains("retry_at <= transaction_timestamp()"));
        assert!(source.contains("(due_micros, kind, execution_id) >"));
        assert!(source.contains("ORDER BY due_micros, kind, execution_id LIMIT $5"));
        assert_eq!(MAX_DUE_WORK_PAGE_SIZE, 1_000);
    }

    #[test]
    fn maintenance_sql_uses_database_time_and_all_mvp_candidate_classes() {
        let source = include_str!("query_executor.rs");
        assert!(source.contains("r.deadline <= transaction_timestamp()"));
        assert!(source.contains("w.expires_at <= transaction_timestamp()"));
        assert!(source.contains("'tool_stale'::text"));
        assert!(source.contains("'agent_stale'::text"));
        assert!(source.contains("(due_micros, kind, target_id) >"));
        assert!(source.contains("ORDER BY due_micros, kind, target_id LIMIT $6"));
        assert_eq!(MAX_MAINTENANCE_PAGE_SIZE, 1_000);
    }

    #[test]
    fn invocation_queries_are_tenant_scoped_and_load_requests() {
        let source = include_str!("query_executor.rs");
        assert!(source.contains("WHERE tenant_id = $1 AND tool_execution_id = $2"));
        assert!(source.contains("WHERE tenant_id = $1 AND agent_execution_id = $2"));
        assert!(source.contains("request_hash, request_json"));
        assert!(source.contains("capabilities_snapshot_json"));
    }
}
