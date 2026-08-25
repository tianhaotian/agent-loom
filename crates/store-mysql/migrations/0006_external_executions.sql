CREATE TABLE tool_executions (
    tool_execution_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    stage_execution_id binary(16) NULL,
    task_id binary(16) NOT NULL,
    tool_call_id varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    tool_name varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    idempotency_scope varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    idempotency_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    request_hash binary(32) NOT NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    attempt_count bigint NOT NULL,
    request_json json NOT NULL,
    result_json json NULL,
    error_code varchar(128) COLLATE utf8mb4_0900_bin NULL,
    recovery_action varchar(128) COLLATE utf8mb4_0900_bin NULL,
    external_ref varchar(512) COLLATE utf8mb4_0900_bin NULL,
    started_at datetime(6) NOT NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    completed_at datetime(6) NULL,
    CONSTRAINT pk_tool_executions PRIMARY KEY (tool_execution_id),
    CONSTRAINT uq_tool_execs__tenant_id UNIQUE (tenant_id, tool_execution_id),
    CONSTRAINT uq_tool_execs__run_id UNIQUE (tenant_id, run_id, tool_execution_id),
    CONSTRAINT uq_tool_execs__call_id UNIQUE (tenant_id, tool_call_id),
    CONSTRAINT uq_tool_execs__idempotency
        UNIQUE (tenant_id, idempotency_scope, idempotency_key),
    CONSTRAINT fk_tool_execs__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_tool_execs__stage
        FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES stage_executions (tenant_id, run_id, stage_execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_tool_execs__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT ck_tool_execs__call_id
        CHECK (char_length(tool_call_id) > 0) ENFORCED,
    CONSTRAINT ck_tool_execs__name CHECK (char_length(tool_name) > 0) ENFORCED,
    CONSTRAINT ck_tool_execs__scope
        CHECK (char_length(idempotency_scope) > 0) ENFORCED,
    CONSTRAINT ck_tool_execs__key
        CHECK (char_length(idempotency_key) > 0) ENFORCED,
    CONSTRAINT ck_tool_execs__status CHECK (status IN (
        'planned', 'executing', 'retry_scheduled', 'succeeded', 'failed',
        'outcome_unknown', 'reconciling', 'compensated', 'manual_review'
    )) ENFORCED,
    CONSTRAINT ck_tool_execs__attempts CHECK (attempt_count >= 0) ENFORCED,
    CONSTRAINT ck_tool_execs__error
        CHECK (error_code IS NULL OR char_length(error_code) > 0) ENFORCED,
    CONSTRAINT ck_tool_execs__recovery CHECK (
        status <> 'outcome_unknown'
        OR (recovery_action IS NOT NULL AND char_length(recovery_action) > 0)
    ) ENFORCED,
    CONSTRAINT ck_tool_execs__external_ref
        CHECK (external_ref IS NULL OR char_length(external_ref) > 0) ENFORCED,
    CONSTRAINT ck_tool_execs__completion CHECK (
        (status IN ('succeeded', 'failed', 'compensated') AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'compensated') AND completed_at IS NULL)
    ) ENFORCED,
    CONSTRAINT ck_tool_execs__time CHECK (
        started_at >= created_at AND updated_at >= created_at
        AND (completed_at IS NULL OR completed_at >= started_at)
    ) ENFORCED,
    INDEX ix_tool_execs__stale (status, updated_at, tool_execution_id),
    INDEX ix_tool_execs__run_status
        (tenant_id, run_id, status, updated_at, tool_execution_id),
    INDEX ix_tool_execs__external_ref (tenant_id, external_ref, tool_execution_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE tool_execution_attempts (
    tool_attempt_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    tool_execution_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    attempt bigint NOT NULL,
    request_started_at datetime(6) NOT NULL,
    request_finished_at datetime(6) NULL,
    adapter_error_code varchar(128) COLLATE utf8mb4_0900_bin NULL,
    retry_class varchar(64) COLLATE utf8mb4_0900_bin NULL,
    remote_request_id varchar(512) COLLATE utf8mb4_0900_bin NULL,
    external_ref varchar(512) COLLATE utf8mb4_0900_bin NULL,
    response_digest binary(32) NULL,
    outcome varchar(32) COLLATE utf8mb4_0900_bin NULL,
    metrics_json json NOT NULL,
    CONSTRAINT pk_tool_attempts PRIMARY KEY (tool_attempt_id),
    CONSTRAINT uq_tool_attempts__tenant_id UNIQUE (tenant_id, tool_attempt_id),
    CONSTRAINT uq_tool_attempts__number
        UNIQUE (tenant_id, tool_execution_id, attempt),
    CONSTRAINT fk_tool_attempts__execution
        FOREIGN KEY (tenant_id, run_id, tool_execution_id)
        REFERENCES tool_executions (tenant_id, run_id, tool_execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_tool_attempts__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_tool_attempts__attempt CHECK (attempt >= 1) ENFORCED,
    CONSTRAINT ck_tool_attempts__retry_class CHECK (
        retry_class IS NULL OR retry_class IN (
            'never', 'same_request_backoff', 'reconnect_and_resume',
            'query_outcome', 'manual_review'
        )
    ) ENFORCED,
    CONSTRAINT ck_tool_attempts__outcome CHECK (
        outcome IS NULL OR outcome IN ('completed', 'accepted', 'uncertain', 'failed')
    ) ENFORCED,
    CONSTRAINT ck_tool_attempts__finalization CHECK (
        (request_finished_at IS NULL AND outcome IS NULL AND adapter_error_code IS NULL
            AND retry_class IS NULL AND response_digest IS NULL)
        OR (request_finished_at IS NOT NULL AND outcome IS NOT NULL)
    ) ENFORCED,
    CONSTRAINT ck_tool_attempts__error
        CHECK (adapter_error_code IS NULL OR char_length(adapter_error_code) > 0) ENFORCED,
    CONSTRAINT ck_tool_attempts__remote_request
        CHECK (remote_request_id IS NULL OR char_length(remote_request_id) > 0) ENFORCED,
    CONSTRAINT ck_tool_attempts__external_ref
        CHECK (external_ref IS NULL OR char_length(external_ref) > 0) ENFORCED,
    CONSTRAINT ck_tool_attempts__time
        CHECK (request_finished_at IS NULL OR request_finished_at >= request_started_at) ENFORCED,
    INDEX ix_tool_attempts__run_execution
        (tenant_id, run_id, tool_execution_id, attempt)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE agent_executions (
    agent_execution_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    stage_execution_id binary(16) NULL,
    task_id binary(16) NOT NULL,
    endpoint_id binary(16) NOT NULL,
    agent_version_id binary(16) NOT NULL,
    idempotency_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    request_hash binary(32) NOT NULL,
    remote_run_ref varchar(512) COLLATE utf8mb4_0900_bin NULL,
    remote_session_ref varchar(512) COLLATE utf8mb4_0900_bin NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    version bigint NOT NULL,
    capabilities_snapshot_json json NOT NULL,
    event_cursor text NULL,
    cursor_version bigint NOT NULL,
    stop_requested_at datetime(6) NULL,
    stop_outcome varchar(64) COLLATE utf8mb4_0900_bin NULL,
    result_json json NULL,
    error_code varchar(128) COLLATE utf8mb4_0900_bin NULL,
    last_synced_at datetime(6) NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    completed_at datetime(6) NULL,
    CONSTRAINT pk_agent_executions PRIMARY KEY (agent_execution_id),
    CONSTRAINT uq_agent_execs__tenant_id UNIQUE (tenant_id, agent_execution_id),
    CONSTRAINT uq_agent_execs__run_id UNIQUE (tenant_id, run_id, agent_execution_id),
    CONSTRAINT uq_agent_execs__idempotency
        UNIQUE (tenant_id, endpoint_id, idempotency_key),
    CONSTRAINT uq_agent_execs__remote_run
        UNIQUE (tenant_id, endpoint_id, remote_run_ref),
    CONSTRAINT fk_agent_execs__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_execs__stage
        FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES stage_executions (tenant_id, run_id, stage_execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_agent_execs__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_execs__endpoint FOREIGN KEY (tenant_id, endpoint_id)
        REFERENCES agent_endpoints (tenant_id, endpoint_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_execs__agent_version
        FOREIGN KEY (tenant_id, agent_version_id)
        REFERENCES agent_definition_versions (tenant_id, agent_version_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_agent_execs__key
        CHECK (char_length(idempotency_key) > 0) ENFORCED,
    CONSTRAINT ck_agent_execs__remote_run
        CHECK (remote_run_ref IS NULL OR char_length(remote_run_ref) > 0) ENFORCED,
    CONSTRAINT ck_agent_execs__remote_session
        CHECK (remote_session_ref IS NULL OR char_length(remote_session_ref) > 0) ENFORCED,
    CONSTRAINT ck_agent_execs__status CHECK (status IN (
        'planned', 'submitting', 'running', 'stopping', 'succeeded', 'failed',
        'cancelled', 'outcome_unknown', 'reconciling', 'manual_review'
    )) ENFORCED,
    CONSTRAINT ck_agent_execs__version CHECK (version >= 0) ENFORCED,
    CONSTRAINT ck_agent_execs__cursor_version CHECK (cursor_version >= 0) ENFORCED,
    CONSTRAINT ck_agent_execs__stop_outcome CHECK (
        stop_outcome IS NULL
        OR (stop_requested_at IS NOT NULL AND char_length(stop_outcome) > 0)
    ) ENFORCED,
    CONSTRAINT ck_agent_execs__error
        CHECK (error_code IS NULL OR char_length(error_code) > 0) ENFORCED,
    CONSTRAINT ck_agent_execs__completion CHECK (
        (status IN ('succeeded', 'failed', 'cancelled') AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'cancelled') AND completed_at IS NULL)
    ) ENFORCED,
    CONSTRAINT ck_agent_execs__time CHECK (
        updated_at >= created_at
        AND (last_synced_at IS NULL OR last_synced_at >= created_at)
        AND (stop_requested_at IS NULL OR stop_requested_at >= created_at)
        AND (completed_at IS NULL OR completed_at >= created_at)
    ) ENFORCED,
    INDEX ix_agent_execs__stale (status, updated_at, agent_execution_id),
    INDEX ix_agent_execs__run_status
        (tenant_id, run_id, status, updated_at, agent_execution_id),
    INDEX ix_agent_execs__session
        (tenant_id, endpoint_id, remote_session_ref, updated_at, agent_execution_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE agent_event_receipts (
    agent_event_receipt_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    agent_execution_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    dedupe_key binary(32) NOT NULL,
    source_event_id varchar(512) COLLATE utf8mb4_0900_bin NULL,
    source_sequence bigint NULL,
    source_cursor text NULL,
    event_kind varchar(128) COLLATE utf8mb4_0900_bin NOT NULL,
    raw_digest binary(32) NOT NULL,
    local_event_id binary(16) NULL,
    recorded_at datetime(6) NOT NULL,
    CONSTRAINT pk_agent_event_receipts PRIMARY KEY (agent_event_receipt_id),
    CONSTRAINT uq_agent_receipts__tenant_id
        UNIQUE (tenant_id, agent_event_receipt_id),
    CONSTRAINT uq_agent_receipts__dedupe
        UNIQUE (tenant_id, agent_execution_id, dedupe_key),
    CONSTRAINT uq_agent_receipts__local_event UNIQUE (tenant_id, local_event_id),
    CONSTRAINT fk_agent_receipts__execution
        FOREIGN KEY (tenant_id, run_id, agent_execution_id)
        REFERENCES agent_executions (tenant_id, run_id, agent_execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_agent_receipts__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_agent_receipts__local_event
        FOREIGN KEY (tenant_id, run_id, local_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_agent_receipts__source_event
        CHECK (source_event_id IS NULL OR char_length(source_event_id) > 0) ENFORCED,
    CONSTRAINT ck_agent_receipts__source_sequence
        CHECK (source_sequence IS NULL OR source_sequence >= 0) ENFORCED,
    CONSTRAINT ck_agent_receipts__kind
        CHECK (char_length(event_kind) > 0) ENFORCED,
    INDEX ix_agent_receipts__source_sequence
        (tenant_id, agent_execution_id, source_sequence, agent_event_receipt_id),
    INDEX ix_agent_receipts__run_time
        (tenant_id, run_id, recorded_at, agent_event_receipt_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;
