ALTER TABLE agent_loom.schedules
    ADD COLUMN misfire_policy varchar(32) COLLATE "C" NOT NULL DEFAULT 'fire_once',
    ADD COLUMN catch_up_limit bigint NOT NULL DEFAULT 1,
    ADD COLUMN next_fire_at timestamptz(6),
    ADD COLUMN last_fire_at timestamptz(6),
    ADD COLUMN version bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_schedules__misfire_policy
        CHECK (misfire_policy IN ('skip', 'fire_once', 'catch_up')),
    ADD CONSTRAINT ck_schedules__catch_up_limit
        CHECK (catch_up_limit BETWEEN 1 AND 100),
    ADD CONSTRAINT ck_schedules__version CHECK (version >= 0);

UPDATE agent_loom.schedules SET next_fire_at = updated_at WHERE next_fire_at IS NULL;

ALTER TABLE agent_loom.schedules ALTER COLUMN next_fire_at SET NOT NULL;

UPDATE agent_loom.command_receipts AS receipt
SET outcome_json = receipt.outcome_json || jsonb_build_object(
    'misfire_policy', schedule.misfire_policy,
    'catch_up_limit', schedule.catch_up_limit,
    'next_fire_at', (extract(epoch FROM schedule.next_fire_at) * 1000000)::bigint,
    'last_fire_at', CASE WHEN schedule.last_fire_at IS NULL THEN NULL
        ELSE (extract(epoch FROM schedule.last_fire_at) * 1000000)::bigint END,
    'version', schedule.version
)
FROM agent_loom.schedules AS schedule
WHERE receipt.tenant_id = schedule.tenant_id
  AND receipt.resource_type = 'schedule'
  AND receipt.resource_id = schedule.schedule_id
  AND receipt.outcome_json ->> 'type' = 'schedule';

CREATE INDEX ix_schedules__due
    ON agent_loom.schedules (tenant_id, status, next_fire_at, schedule_id);
