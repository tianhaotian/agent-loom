CREATE TABLE agent_loom.tool_executions (
    tool_execution_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    stage_execution_id uuid,
    task_id uuid NOT NULL,
    tool_call_id varchar(255) COLLATE "C" NOT NULL,
    tool_name varchar(255) COLLATE "C" NOT NULL,
    idempotency_scope varchar(255) COLLATE "C" NOT NULL,
    idempotency_key varchar(255) COLLATE "C" NOT NULL,
    request_hash bytea NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    attempt_count bigint NOT NULL,
    request_json jsonb NOT NULL,
    result_json jsonb,
    error_code varchar(128) COLLATE "C",
    recovery_action varchar(128) COLLATE "C",
    external_ref varchar(512) COLLATE "C",
    started_at timestamptz(6) NOT NULL,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    completed_at timestamptz(6),
    CONSTRAINT pk_tool_executions PRIMARY KEY (tool_execution_id),
    CONSTRAINT uq_tool_execs__tenant_id UNIQUE (tenant_id, tool_execution_id),
    CONSTRAINT uq_tool_execs__run_id UNIQUE (tenant_id, run_id, tool_execution_id),
    CONSTRAINT uq_tool_execs__call_id UNIQUE (tenant_id, tool_call_id),
    CONSTRAINT uq_tool_execs__idempotency
        UNIQUE (tenant_id, idempotency_scope, idempotency_key),
    CONSTRAINT fk_tool_execs__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_tool_execs__stage
        FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES agent_loom.stage_executions
            (tenant_id, run_id, stage_execution_id) ON DELETE RESTRICT,
    CONSTRAINT fk_tool_execs__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES agent_loom.tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT ck_tool_execs__call_id CHECK (length(tool_call_id) > 0),
    CONSTRAINT ck_tool_execs__name CHECK (length(tool_name) > 0),
    CONSTRAINT ck_tool_execs__scope CHECK (length(idempotency_scope) > 0),
    CONSTRAINT ck_tool_execs__key CHECK (length(idempotency_key) > 0),
    CONSTRAINT ck_tool_execs__hash CHECK (octet_length(request_hash) = 32),
    CONSTRAINT ck_tool_execs__status CHECK (status IN (
        'planned', 'executing', 'retry_scheduled', 'succeeded', 'failed',
        'outcome_unknown', 'reconciling', 'compensated', 'manual_review'
    )),
    CONSTRAINT ck_tool_execs__attempts CHECK (attempt_count >= 0),
    CONSTRAINT ck_tool_execs__error
        CHECK (error_code IS NULL OR length(error_code) > 0),
    CONSTRAINT ck_tool_execs__recovery CHECK (
        status <> 'outcome_unknown'
        OR (recovery_action IS NOT NULL AND length(recovery_action) > 0)
    ),
    CONSTRAINT ck_tool_execs__external_ref
        CHECK (external_ref IS NULL OR length(external_ref) > 0),
    CONSTRAINT ck_tool_execs__completion CHECK (
        (status IN ('succeeded', 'failed', 'compensated') AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'compensated') AND completed_at IS NULL)
    ),
    CONSTRAINT ck_tool_execs__time CHECK (
        started_at >= created_at AND updated_at >= created_at
        AND (completed_at IS NULL OR completed_at >= started_at)
    )
);

CREATE INDEX ix_tool_execs__stale
    ON agent_loom.tool_executions (status, updated_at, tool_execution_id);
CREATE INDEX ix_tool_execs__run_status
    ON agent_loom.tool_executions
        (tenant_id, run_id, status, updated_at, tool_execution_id);
CREATE INDEX ix_tool_execs__external_ref
    ON agent_loom.tool_executions (tenant_id, external_ref, tool_execution_id);

CREATE TABLE agent_loom.tool_execution_attempts (
    tool_attempt_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    tool_execution_id uuid NOT NULL,
    run_id uuid NOT NULL,
    attempt bigint NOT NULL,
    request_started_at timestamptz(6) NOT NULL,
    request_finished_at timestamptz(6),
    adapter_error_code varchar(128) COLLATE "C",
    retry_class varchar(64) COLLATE "C",
    remote_request_id varchar(512) COLLATE "C",
    external_ref varchar(512) COLLATE "C",
    response_digest bytea,
    outcome varchar(32) COLLATE "C",
    metrics_json jsonb NOT NULL,
    CONSTRAINT pk_tool_attempts PRIMARY KEY (tool_attempt_id),
    CONSTRAINT uq_tool_attempts__tenant_id UNIQUE (tenant_id, tool_attempt_id),
    CONSTRAINT uq_tool_attempts__number
        UNIQUE (tenant_id, tool_execution_id, attempt),
    CONSTRAINT fk_tool_attempts__execution
        FOREIGN KEY (tenant_id, run_id, tool_execution_id)
        REFERENCES agent_loom.tool_executions
            (tenant_id, run_id, tool_execution_id) ON DELETE RESTRICT,
    CONSTRAINT fk_tool_attempts__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_tool_attempts__attempt CHECK (attempt >= 1),
    CONSTRAINT ck_tool_attempts__retry_class CHECK (
        retry_class IS NULL OR retry_class IN (
            'never', 'same_request_backoff', 'reconnect_and_resume',
            'query_outcome', 'manual_review'
        )
    ),
    CONSTRAINT ck_tool_attempts__outcome CHECK (
        outcome IS NULL OR outcome IN ('completed', 'accepted', 'uncertain', 'failed')
    ),
    CONSTRAINT ck_tool_attempts__finalization CHECK (
        (request_finished_at IS NULL AND outcome IS NULL AND adapter_error_code IS NULL
            AND retry_class IS NULL AND response_digest IS NULL)
        OR (request_finished_at IS NOT NULL AND outcome IS NOT NULL)
    ),
    CONSTRAINT ck_tool_attempts__error
        CHECK (adapter_error_code IS NULL OR length(adapter_error_code) > 0),
    CONSTRAINT ck_tool_attempts__remote_request
        CHECK (remote_request_id IS NULL OR length(remote_request_id) > 0),
    CONSTRAINT ck_tool_attempts__external_ref
        CHECK (external_ref IS NULL OR length(external_ref) > 0),
    CONSTRAINT ck_tool_attempts__digest
        CHECK (response_digest IS NULL OR octet_length(response_digest) = 32),
    CONSTRAINT ck_tool_attempts__time
        CHECK (request_finished_at IS NULL OR request_finished_at >= request_started_at)
);

CREATE INDEX ix_tool_attempts__run_execution
    ON agent_loom.tool_execution_attempts
        (tenant_id, run_id, tool_execution_id, attempt);

CREATE TABLE agent_loom.agent_executions (
    agent_execution_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    stage_execution_id uuid,
    task_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    agent_version_id uuid NOT NULL,
    idempotency_key varchar(255) COLLATE "C" NOT NULL,
    request_hash bytea NOT NULL,
    remote_run_ref varchar(512) COLLATE "C",
    remote_session_ref varchar(512) COLLATE "C",
    status varchar(32) COLLATE "C" NOT NULL,
    version bigint NOT NULL,
    capabilities_snapshot_json jsonb NOT NULL,
    event_cursor text,
    cursor_version bigint NOT NULL,
    stop_requested_at timestamptz(6),
    stop_outcome varchar(64) COLLATE "C",
    result_json jsonb,
    error_code varchar(128) COLLATE "C",
    last_synced_at timestamptz(6),
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    completed_at timestamptz(6),
    CONSTRAINT pk_agent_executions PRIMARY KEY (agent_execution_id),
    CONSTRAINT uq_agent_execs__tenant_id UNIQUE (tenant_id, agent_execution_id),
    CONSTRAINT uq_agent_execs__run_id UNIQUE (tenant_id, run_id, agent_execution_id),
    CONSTRAINT uq_agent_execs__idempotency
        UNIQUE (tenant_id, endpoint_id, idempotency_key),
    CONSTRAINT uq_agent_execs__remote_run
        UNIQUE (tenant_id, endpoint_id, remote_run_ref),
    CONSTRAINT fk_agent_execs__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_execs__stage
        FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES agent_loom.stage_executions
            (tenant_id, run_id, stage_execution_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_execs__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES agent_loom.tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_execs__endpoint FOREIGN KEY (tenant_id, endpoint_id)
        REFERENCES agent_loom.agent_endpoints (tenant_id, endpoint_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_agent_execs__agent_version
        FOREIGN KEY (tenant_id, agent_version_id)
        REFERENCES agent_loom.agent_definition_versions (tenant_id, agent_version_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_agent_execs__key CHECK (length(idempotency_key) > 0),
    CONSTRAINT ck_agent_execs__hash CHECK (octet_length(request_hash) = 32),
    CONSTRAINT ck_agent_execs__remote_run
        CHECK (remote_run_ref IS NULL OR length(remote_run_ref) > 0),
    CONSTRAINT ck_agent_execs__remote_session
        CHECK (remote_session_ref IS NULL OR length(remote_session_ref) > 0),
    CONSTRAINT ck_agent_execs__status CHECK (status IN (
        'planned', 'submitting', 'running', 'stopping', 'succeeded', 'failed',
        'cancelled', 'outcome_unknown', 'reconciling', 'manual_review'
    )),
    CONSTRAINT ck_agent_execs__version CHECK (version >= 0),
    CONSTRAINT ck_agent_execs__cursor_version CHECK (cursor_version >= 0),
    CONSTRAINT ck_agent_execs__stop_outcome CHECK (
        stop_outcome IS NULL
        OR (stop_requested_at IS NOT NULL AND length(stop_outcome) > 0)
    ),
    CONSTRAINT ck_agent_execs__error
        CHECK (error_code IS NULL OR length(error_code) > 0),
    CONSTRAINT ck_agent_execs__completion CHECK (
        (status IN ('succeeded', 'failed', 'cancelled') AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'cancelled') AND completed_at IS NULL)
    ),
    CONSTRAINT ck_agent_execs__time CHECK (
        updated_at >= created_at
        AND (last_synced_at IS NULL OR last_synced_at >= created_at)
        AND (stop_requested_at IS NULL OR stop_requested_at >= created_at)
        AND (completed_at IS NULL OR completed_at >= created_at)
    )
);

CREATE INDEX ix_agent_execs__stale
    ON agent_loom.agent_executions (status, updated_at, agent_execution_id);
CREATE INDEX ix_agent_execs__run_status
    ON agent_loom.agent_executions
        (tenant_id, run_id, status, updated_at, agent_execution_id);
CREATE INDEX ix_agent_execs__session
    ON agent_loom.agent_executions
        (tenant_id, endpoint_id, remote_session_ref, updated_at, agent_execution_id);

CREATE TABLE agent_loom.agent_event_receipts (
    agent_event_receipt_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    agent_execution_id uuid NOT NULL,
    run_id uuid NOT NULL,
    dedupe_key bytea NOT NULL,
    source_event_id varchar(512) COLLATE "C",
    source_sequence bigint,
    source_cursor text,
    event_kind varchar(128) COLLATE "C" NOT NULL,
    raw_digest bytea NOT NULL,
    local_event_id uuid,
    recorded_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_agent_event_receipts PRIMARY KEY (agent_event_receipt_id),
    CONSTRAINT uq_agent_receipts__tenant_id
        UNIQUE (tenant_id, agent_event_receipt_id),
    CONSTRAINT uq_agent_receipts__dedupe
        UNIQUE (tenant_id, agent_execution_id, dedupe_key),
    CONSTRAINT uq_agent_receipts__local_event UNIQUE (tenant_id, local_event_id),
    CONSTRAINT fk_agent_receipts__execution
        FOREIGN KEY (tenant_id, run_id, agent_execution_id)
        REFERENCES agent_loom.agent_executions
            (tenant_id, run_id, agent_execution_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_receipts__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_receipts__local_event
        FOREIGN KEY (tenant_id, run_id, local_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_agent_receipts__dedupe CHECK (octet_length(dedupe_key) = 32),
    CONSTRAINT ck_agent_receipts__source_event
        CHECK (source_event_id IS NULL OR length(source_event_id) > 0),
    CONSTRAINT ck_agent_receipts__source_sequence
        CHECK (source_sequence IS NULL OR source_sequence >= 0),
    CONSTRAINT ck_agent_receipts__kind CHECK (length(event_kind) > 0),
    CONSTRAINT ck_agent_receipts__raw_digest CHECK (octet_length(raw_digest) = 32)
);

CREATE INDEX ix_agent_receipts__source_sequence
    ON agent_loom.agent_event_receipts
        (tenant_id, agent_execution_id, source_sequence, agent_event_receipt_id);
CREATE INDEX ix_agent_receipts__run_time
    ON agent_loom.agent_event_receipts
        (tenant_id, run_id, recorded_at, agent_event_receipt_id);
