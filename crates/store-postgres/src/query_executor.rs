use agent_loom_domain::{
    CheckpointId, EventRecord, JsonPayload, RunId, RunSnapshot, TenantId, UnixMicros,
};
use agent_loom_durable_store::{
    EventCursor, EventPage, QueryContext, StoreError, StoreErrorCode, StoreResult,
};
use serde_json::Value;
use tokio_postgres::{Client, Row};
use uuid::Uuid;

use crate::{
    event_id_from_uuid, inconsistent, map_database_error, nonnegative_u64, parse_run_status,
    run_id_from_uuid, uuid, workflow_id_from_uuid,
};

const MAX_EVENT_PAGE_SIZE: u32 = 1_000;

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
}
