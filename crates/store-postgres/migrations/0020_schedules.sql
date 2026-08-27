CREATE TABLE agent_loom.schedules (
    schedule_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    workflow_version_id uuid NOT NULL,
    cron_expression varchar(255) COLLATE "C" NOT NULL,
    timezone varchar(128) COLLATE "C" NOT NULL,
    input_json jsonb NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    created_by varchar(512) COLLATE "C" NOT NULL,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_schedules PRIMARY KEY (schedule_id),
    CONSTRAINT uq_schedules__tenant UNIQUE (tenant_id, schedule_id),
    CONSTRAINT fk_schedules__tenant FOREIGN KEY (tenant_id)
        REFERENCES agent_loom.tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT fk_schedules__workflow_version
        FOREIGN KEY (tenant_id, workflow_version_id)
        REFERENCES agent_loom.workflow_definition_versions
            (tenant_id, workflow_version_id) ON DELETE RESTRICT,
    CONSTRAINT ck_schedules__cron CHECK (length(cron_expression) > 0),
    CONSTRAINT ck_schedules__timezone CHECK (length(timezone) > 0),
    CONSTRAINT ck_schedules__status CHECK (status IN ('active', 'paused')),
    CONSTRAINT ck_schedules__creator CHECK (length(created_by) > 0),
    CONSTRAINT ck_schedules__time CHECK (updated_at >= created_at)
);

CREATE INDEX ix_schedules__status
    ON agent_loom.schedules (tenant_id, status, created_at, schedule_id);

ALTER TABLE agent_loom.runs
    ADD COLUMN schedule_id uuid,
    ADD COLUMN scheduled_fire_at timestamptz(6),
    ADD CONSTRAINT fk_runs__schedule FOREIGN KEY (tenant_id, schedule_id)
        REFERENCES agent_loom.schedules (tenant_id, schedule_id) ON DELETE RESTRICT,
    ADD CONSTRAINT uq_runs__schedule_fire
        UNIQUE (tenant_id, schedule_id, scheduled_fire_at),
    ADD CONSTRAINT ck_runs__schedule_fire CHECK (
        (schedule_id IS NULL AND scheduled_fire_at IS NULL)
        OR (schedule_id IS NOT NULL AND scheduled_fire_at IS NOT NULL)
    );

CREATE INDEX ix_runs__schedule_fire
    ON agent_loom.runs (tenant_id, schedule_id, scheduled_fire_at, run_id);
