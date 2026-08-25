CREATE TABLE runs (
    run_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    workflow_version_id binary(16) NULL,
    coordinator_agent_version_id binary(16) NULL,
    parent_run_id binary(16) NULL,
    parent_task_id binary(16) NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    suspended_from_status varchar(32) COLLATE utf8mb4_0900_bin NULL,
    version bigint NOT NULL,
    execution_generation bigint NOT NULL,
    next_event_sequence bigint NOT NULL,
    current_checkpoint_id binary(16) NULL,
    terminal_event_id binary(16) NULL,
    input_json json NOT NULL,
    state_summary_json json NOT NULL,
    deadline datetime(6) NULL,
    resume_blocked_reason varchar(512) NULL,
    created_by varchar(512) COLLATE utf8mb4_0900_bin NOT NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    terminal_at datetime(6) NULL,
    CONSTRAINT pk_runs PRIMARY KEY (run_id),
    CONSTRAINT uq_runs__tenant_id UNIQUE (tenant_id, run_id),
    CONSTRAINT fk_runs__tenant FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT fk_runs__workflow_version
        FOREIGN KEY (tenant_id, workflow_version_id)
        REFERENCES workflow_definition_versions (tenant_id, workflow_version_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_runs__coordinator_version
        FOREIGN KEY (tenant_id, coordinator_agent_version_id)
        REFERENCES agent_definition_versions (tenant_id, agent_version_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_runs__parent_run FOREIGN KEY (tenant_id, parent_run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_runs__status CHECK (status IN (
        'queued', 'running', 'waiting', 'approval_required', 'retrying',
        'paused', 'completed', 'failed', 'cancelled', 'timed_out'
    )) ENFORCED,
    CONSTRAINT ck_runs__suspended_from CHECK (
        (status = 'paused' AND suspended_from_status IN (
            'queued', 'running', 'waiting', 'approval_required', 'retrying'
        ))
        OR (status <> 'paused' AND suspended_from_status IS NULL)
    ) ENFORCED,
    CONSTRAINT ck_runs__version CHECK (version >= 0) ENFORCED,
    CONSTRAINT ck_runs__generation CHECK (execution_generation >= 0) ENFORCED,
    CONSTRAINT ck_runs__event_sequence CHECK (next_event_sequence >= 1) ENFORCED,
    CONSTRAINT ck_runs__parent
        CHECK (parent_run_id IS NULL OR parent_run_id <> run_id) ENFORCED,
    CONSTRAINT ck_runs__deadline
        CHECK (deadline IS NULL OR deadline >= created_at) ENFORCED,
    CONSTRAINT ck_runs__creator CHECK (char_length(created_by) > 0) ENFORCED,
    CONSTRAINT ck_runs__time CHECK (updated_at >= created_at) ENFORCED,
    CONSTRAINT ck_runs__terminal_projection CHECK (
        (status IN ('completed', 'failed', 'cancelled', 'timed_out')
            AND terminal_event_id IS NOT NULL AND terminal_at IS NOT NULL)
        OR
        (status NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
            AND terminal_event_id IS NULL AND terminal_at IS NULL)
    ) ENFORCED,
    INDEX ix_runs__tenant_status_page (tenant_id, status, updated_at, run_id),
    INDEX ix_runs__workflow_page
        (tenant_id, workflow_version_id, created_at, run_id),
    INDEX ix_runs__parent (tenant_id, parent_run_id, created_at, run_id),
    INDEX ix_runs__deadline (status, deadline, run_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE events (
    event_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    sequence bigint NOT NULL,
    event_type varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    payload_json json NOT NULL,
    payload_schema_version bigint NOT NULL,
    producer varchar(128) COLLATE utf8mb4_0900_bin NOT NULL,
    actor_ref varchar(512) COLLATE utf8mb4_0900_bin NOT NULL,
    correlation_id binary(16) NOT NULL,
    causation_id binary(16) NULL,
    idempotency_key varchar(255) COLLATE utf8mb4_0900_bin NULL,
    occurred_at datetime(6) NULL,
    recorded_at datetime(6) NOT NULL,
    CONSTRAINT pk_events PRIMARY KEY (event_id),
    CONSTRAINT uq_events__tenant_id UNIQUE (tenant_id, event_id),
    CONSTRAINT uq_events__run_id UNIQUE (tenant_id, run_id, event_id),
    CONSTRAINT uq_events__sequence UNIQUE (tenant_id, run_id, sequence),
    CONSTRAINT fk_events__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_events__sequence CHECK (sequence >= 1) ENFORCED,
    CONSTRAINT ck_events__type_nonempty
        CHECK (char_length(event_type) > 0) ENFORCED,
    CONSTRAINT ck_events__payload_version
        CHECK (payload_schema_version >= 1) ENFORCED,
    CONSTRAINT ck_events__producer CHECK (char_length(producer) > 0) ENFORCED,
    CONSTRAINT ck_events__actor CHECK (char_length(actor_ref) > 0) ENFORCED,
    CONSTRAINT ck_events__idempotency_key
        CHECK (idempotency_key IS NULL OR char_length(idempotency_key) > 0) ENFORCED,
    INDEX ix_events__correlation
        (tenant_id, correlation_id, recorded_at, event_id),
    INDEX ix_events__type_time
        (tenant_id, event_type, recorded_at, event_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

ALTER TABLE runs
    ADD CONSTRAINT fk_runs__terminal_event
    FOREIGN KEY (tenant_id, run_id, terminal_event_id)
    REFERENCES events (tenant_id, run_id, event_id)
    ON DELETE RESTRICT;

CREATE TABLE command_receipts (
    receipt_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    scope varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    idempotency_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    request_hash binary(32) NOT NULL,
    outcome_kind varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    outcome_json json NOT NULL,
    event_id binary(16) NULL,
    resource_type varchar(64) COLLATE utf8mb4_0900_bin NULL,
    resource_id binary(16) NULL,
    resource_version bigint NULL,
    created_at datetime(6) NOT NULL,
    expires_at datetime(6) NOT NULL,
    CONSTRAINT pk_command_receipts PRIMARY KEY (receipt_id),
    CONSTRAINT uq_receipts__tenant_id UNIQUE (tenant_id, receipt_id),
    CONSTRAINT uq_receipts__idempotency UNIQUE (tenant_id, scope, idempotency_key),
    CONSTRAINT fk_receipts__tenant FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT fk_receipts__event FOREIGN KEY (tenant_id, event_id)
        REFERENCES events (tenant_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_receipts__scope CHECK (char_length(scope) > 0) ENFORCED,
    CONSTRAINT ck_receipts__key
        CHECK (char_length(idempotency_key) > 0) ENFORCED,
    CONSTRAINT ck_receipts__outcome CHECK (outcome_kind IN (
        'applied', 'duplicate', 'no_op', 'rejected', 'conflict', 'outcome_unknown'
    )) ENFORCED,
    CONSTRAINT ck_receipts__resource CHECK (
        (resource_type IS NULL AND resource_id IS NULL)
        OR (resource_type IS NOT NULL AND char_length(resource_type) > 0
            AND resource_id IS NOT NULL)
    ) ENFORCED,
    CONSTRAINT ck_receipts__resource_version
        CHECK (resource_version IS NULL OR resource_version >= 0) ENFORCED,
    CONSTRAINT ck_receipts__expiry CHECK (expires_at >= created_at) ENFORCED,
    INDEX ix_receipts__resource
        (tenant_id, resource_type, resource_id, created_at, receipt_id),
    INDEX ix_receipts__expiry (expires_at, receipt_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;
