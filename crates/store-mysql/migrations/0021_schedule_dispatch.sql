ALTER TABLE schedules
    ADD COLUMN misfire_policy varchar(32) CHARACTER SET ascii COLLATE ascii_bin
        NOT NULL DEFAULT 'fire_once',
    ADD COLUMN catch_up_limit bigint NOT NULL DEFAULT 1,
    ADD COLUMN next_fire_at datetime(6),
    ADD COLUMN last_fire_at datetime(6),
    ADD COLUMN version bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_schedules__misfire_policy
        CHECK (misfire_policy IN ('skip', 'fire_once', 'catch_up')) ENFORCED,
    ADD CONSTRAINT ck_schedules__catch_up_limit
        CHECK (catch_up_limit BETWEEN 1 AND 100) ENFORCED,
    ADD CONSTRAINT ck_schedules__version CHECK (version >= 0) ENFORCED;

UPDATE schedules SET next_fire_at = updated_at WHERE next_fire_at IS NULL;

ALTER TABLE schedules MODIFY COLUMN next_fire_at datetime(6) NOT NULL;

UPDATE command_receipts AS receipt
JOIN schedules AS schedule
  ON receipt.tenant_id = schedule.tenant_id
 AND receipt.resource_type = 'schedule'
 AND receipt.resource_id = schedule.schedule_id
SET receipt.outcome_json = JSON_SET(
    receipt.outcome_json,
    '$.misfire_policy', schedule.misfire_policy,
    '$.catch_up_limit', schedule.catch_up_limit,
    '$.next_fire_at', CAST(UNIX_TIMESTAMP(schedule.next_fire_at) * 1000000 AS SIGNED),
    '$.last_fire_at', CASE WHEN schedule.last_fire_at IS NULL THEN NULL
        ELSE CAST(UNIX_TIMESTAMP(schedule.last_fire_at) * 1000000 AS SIGNED) END,
    '$.version', schedule.version
)
WHERE JSON_UNQUOTE(JSON_EXTRACT(receipt.outcome_json, '$.type')) = 'schedule';

CREATE INDEX ix_schedules__due
    ON schedules (tenant_id, status, next_fire_at, schedule_id);
