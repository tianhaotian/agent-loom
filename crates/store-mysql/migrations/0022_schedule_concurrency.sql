ALTER TABLE schedules
    ADD COLUMN concurrency_policy varchar(32) CHARACTER SET ascii COLLATE ascii_bin
        NOT NULL DEFAULT 'allow',
    ADD CONSTRAINT ck_schedules__concurrency_policy
        CHECK (concurrency_policy IN ('allow', 'forbid')) ENFORCED;

UPDATE command_receipts AS receipt
JOIN schedules AS schedule
  ON receipt.tenant_id = schedule.tenant_id
 AND receipt.resource_type = 'schedule'
 AND receipt.resource_id = schedule.schedule_id
SET receipt.outcome_json = JSON_SET(
    receipt.outcome_json,
    '$.concurrency_policy', schedule.concurrency_policy
)
WHERE JSON_UNQUOTE(JSON_EXTRACT(receipt.outcome_json, '$.type')) = 'schedule';

CREATE INDEX ix_runs__schedule_active
    ON runs (tenant_id, schedule_id, status, created_at, run_id);
