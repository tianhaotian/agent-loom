CREATE TABLE schedules (
    schedule_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    workflow_version_id binary(16) NOT NULL,
    cron_expression varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    timezone varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    input_json json NOT NULL,
    status varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_by varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    CONSTRAINT pk_schedules PRIMARY KEY (schedule_id),
    CONSTRAINT uq_schedules__tenant UNIQUE (tenant_id, schedule_id),
    CONSTRAINT fk_schedules__tenant FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT fk_schedules__workflow_version
        FOREIGN KEY (tenant_id, workflow_version_id)
        REFERENCES workflow_definition_versions
            (tenant_id, workflow_version_id) ON DELETE RESTRICT,
    CONSTRAINT ck_schedules__cron CHECK (char_length(cron_expression) > 0) ENFORCED,
    CONSTRAINT ck_schedules__timezone CHECK (char_length(timezone) > 0) ENFORCED,
    CONSTRAINT ck_schedules__status CHECK (status IN ('active', 'paused')) ENFORCED,
    CONSTRAINT ck_schedules__creator CHECK (char_length(created_by) > 0) ENFORCED,
    CONSTRAINT ck_schedules__time CHECK (updated_at >= created_at) ENFORCED,
    INDEX ix_schedules__status (tenant_id, status, created_at, schedule_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_as_cs;

ALTER TABLE runs
    ADD COLUMN schedule_id binary(16),
    ADD COLUMN scheduled_fire_at datetime(6),
    ADD CONSTRAINT fk_runs__schedule FOREIGN KEY (tenant_id, schedule_id)
        REFERENCES schedules (tenant_id, schedule_id) ON DELETE RESTRICT,
    ADD CONSTRAINT uq_runs__schedule_fire
        UNIQUE (tenant_id, schedule_id, scheduled_fire_at),
    ADD CONSTRAINT ck_runs__schedule_fire CHECK (
        (schedule_id IS NULL AND scheduled_fire_at IS NULL)
        OR (schedule_id IS NOT NULL AND scheduled_fire_at IS NOT NULL)
    ) ENFORCED,
    ADD INDEX ix_runs__schedule_fire
        (tenant_id, schedule_id, scheduled_fire_at, run_id);
