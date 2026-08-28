ALTER TABLE agent_loom.schedules
    ADD COLUMN concurrency_policy varchar(32) COLLATE "C" NOT NULL DEFAULT 'allow',
    ADD CONSTRAINT ck_schedules__concurrency_policy
        CHECK (concurrency_policy IN ('allow', 'forbid'));

UPDATE agent_loom.command_receipts AS receipt
SET outcome_json = receipt.outcome_json || jsonb_build_object(
    'concurrency_policy', schedule.concurrency_policy
)
FROM agent_loom.schedules AS schedule
WHERE receipt.tenant_id = schedule.tenant_id
  AND receipt.resource_type = 'schedule'
  AND receipt.resource_id = schedule.schedule_id
  AND receipt.outcome_json ->> 'type' = 'schedule';

CREATE INDEX ix_runs__schedule_active
    ON agent_loom.runs (tenant_id, schedule_id, status, created_at, run_id)
    WHERE schedule_id IS NOT NULL
      AND status IN ('queued', 'running', 'waiting', 'approval_required', 'retrying', 'paused');
