CREATE TABLE agent_loom.runs (
    run_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    workflow_version_id uuid,
    coordinator_agent_version_id uuid,
    parent_run_id uuid,
    parent_task_id uuid,
    status varchar(32) COLLATE "C" NOT NULL,
    suspended_from_status varchar(32) COLLATE "C",
    version bigint NOT NULL,
    execution_generation bigint NOT NULL,
    next_event_sequence bigint NOT NULL,
    current_checkpoint_id uuid,
    terminal_event_id uuid,
    input_json jsonb NOT NULL,
    state_summary_json jsonb NOT NULL,
    deadline timestamptz(6),
    resume_blocked_reason varchar(512),
    created_by varchar(512) COLLATE "C" NOT NULL,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    terminal_at timestamptz(6),
    CONSTRAINT pk_runs PRIMARY KEY (run_id),
    CONSTRAINT uq_runs__tenant_id UNIQUE (tenant_id, run_id),
    CONSTRAINT fk_runs__tenant FOREIGN KEY (tenant_id)
        REFERENCES agent_loom.tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT fk_runs__workflow_version
        FOREIGN KEY (tenant_id, workflow_version_id)
        REFERENCES agent_loom.workflow_definition_versions
            (tenant_id, workflow_version_id) ON DELETE RESTRICT,
    CONSTRAINT fk_runs__coordinator_version
        FOREIGN KEY (tenant_id, coordinator_agent_version_id)
        REFERENCES agent_loom.agent_definition_versions
            (tenant_id, agent_version_id) ON DELETE RESTRICT,
    CONSTRAINT fk_runs__parent_run FOREIGN KEY (tenant_id, parent_run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_runs__status CHECK (status IN (
        'queued', 'running', 'waiting', 'approval_required', 'retrying',
        'paused', 'completed', 'failed', 'cancelled', 'timed_out'
    )),
    CONSTRAINT ck_runs__suspended_from CHECK (
        (status = 'paused' AND suspended_from_status IN (
            'queued', 'running', 'waiting', 'approval_required', 'retrying'
        ))
        OR (status <> 'paused' AND suspended_from_status IS NULL)
    ),
    CONSTRAINT ck_runs__version CHECK (version >= 0),
    CONSTRAINT ck_runs__generation CHECK (execution_generation >= 0),
    CONSTRAINT ck_runs__event_sequence CHECK (next_event_sequence >= 1),
    CONSTRAINT ck_runs__parent CHECK (parent_run_id IS NULL OR parent_run_id <> run_id),
    CONSTRAINT ck_runs__deadline CHECK (deadline IS NULL OR deadline >= created_at),
    CONSTRAINT ck_runs__creator CHECK (length(created_by) > 0),
    CONSTRAINT ck_runs__time CHECK (updated_at >= created_at),
    CONSTRAINT ck_runs__terminal_projection CHECK (
        (status IN ('completed', 'failed', 'cancelled', 'timed_out')
            AND terminal_event_id IS NOT NULL AND terminal_at IS NOT NULL)
        OR
        (status NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
            AND terminal_event_id IS NULL AND terminal_at IS NULL)
    )
);

CREATE INDEX ix_runs__tenant_status_page
    ON agent_loom.runs (tenant_id, status, updated_at, run_id);
CREATE INDEX ix_runs__workflow_page
    ON agent_loom.runs (tenant_id, workflow_version_id, created_at, run_id);
CREATE INDEX ix_runs__parent
    ON agent_loom.runs (tenant_id, parent_run_id, created_at, run_id);
CREATE INDEX ix_runs__deadline
    ON agent_loom.runs (status, deadline, run_id);

CREATE TABLE agent_loom.events (
    event_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    sequence bigint NOT NULL,
    event_type varchar(255) COLLATE "C" NOT NULL,
    payload_json jsonb NOT NULL,
    payload_schema_version bigint NOT NULL,
    producer varchar(128) COLLATE "C" NOT NULL,
    actor_ref varchar(512) COLLATE "C" NOT NULL,
    correlation_id uuid NOT NULL,
    causation_id uuid,
    idempotency_key varchar(255) COLLATE "C",
    occurred_at timestamptz(6),
    recorded_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_events PRIMARY KEY (event_id),
    CONSTRAINT uq_events__tenant_id UNIQUE (tenant_id, event_id),
    CONSTRAINT uq_events__run_id UNIQUE (tenant_id, run_id, event_id),
    CONSTRAINT uq_events__sequence UNIQUE (tenant_id, run_id, sequence),
    CONSTRAINT fk_events__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_events__sequence CHECK (sequence >= 1),
    CONSTRAINT ck_events__type_nonempty CHECK (length(event_type) > 0),
    CONSTRAINT ck_events__payload_version CHECK (payload_schema_version >= 1),
    CONSTRAINT ck_events__producer CHECK (length(producer) > 0),
    CONSTRAINT ck_events__actor CHECK (length(actor_ref) > 0),
    CONSTRAINT ck_events__idempotency_key
        CHECK (idempotency_key IS NULL OR length(idempotency_key) > 0)
);

CREATE INDEX ix_events__correlation
    ON agent_loom.events (tenant_id, correlation_id, recorded_at, event_id);
CREATE INDEX ix_events__type_time
    ON agent_loom.events (tenant_id, event_type, recorded_at, event_id);

ALTER TABLE agent_loom.runs
    ADD CONSTRAINT fk_runs__terminal_event
    FOREIGN KEY (tenant_id, run_id, terminal_event_id)
    REFERENCES agent_loom.events (tenant_id, run_id, event_id)
    ON DELETE RESTRICT;

CREATE TABLE agent_loom.command_receipts (
    receipt_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    scope varchar(255) COLLATE "C" NOT NULL,
    idempotency_key varchar(255) COLLATE "C" NOT NULL,
    request_hash bytea NOT NULL,
    outcome_kind varchar(32) COLLATE "C" NOT NULL,
    outcome_json jsonb NOT NULL,
    event_id uuid,
    resource_type varchar(64) COLLATE "C",
    resource_id uuid,
    resource_version bigint,
    created_at timestamptz(6) NOT NULL,
    expires_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_command_receipts PRIMARY KEY (receipt_id),
    CONSTRAINT uq_receipts__tenant_id UNIQUE (tenant_id, receipt_id),
    CONSTRAINT uq_receipts__idempotency UNIQUE (tenant_id, scope, idempotency_key),
    CONSTRAINT fk_receipts__tenant FOREIGN KEY (tenant_id)
        REFERENCES agent_loom.tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT fk_receipts__event FOREIGN KEY (tenant_id, event_id)
        REFERENCES agent_loom.events (tenant_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_receipts__scope CHECK (length(scope) > 0),
    CONSTRAINT ck_receipts__key CHECK (length(idempotency_key) > 0),
    CONSTRAINT ck_receipts__hash CHECK (octet_length(request_hash) = 32),
    CONSTRAINT ck_receipts__outcome CHECK (outcome_kind IN (
        'applied', 'duplicate', 'no_op', 'rejected', 'conflict', 'outcome_unknown'
    )),
    CONSTRAINT ck_receipts__resource CHECK (
        (resource_type IS NULL AND resource_id IS NULL)
        OR (resource_type IS NOT NULL AND length(resource_type) > 0 AND resource_id IS NOT NULL)
    ),
    CONSTRAINT ck_receipts__resource_version
        CHECK (resource_version IS NULL OR resource_version >= 0),
    CONSTRAINT ck_receipts__expiry CHECK (expires_at >= created_at)
);

CREATE INDEX ix_receipts__resource
    ON agent_loom.command_receipts
        (tenant_id, resource_type, resource_id, created_at, receipt_id);
CREATE INDEX ix_receipts__expiry
    ON agent_loom.command_receipts (expires_at, receipt_id);
