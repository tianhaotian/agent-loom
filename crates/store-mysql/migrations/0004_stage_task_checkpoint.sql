CREATE TABLE stage_executions (
    stage_execution_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    stage_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    definition_stage_key varchar(255) COLLATE utf8mb4_0900_bin NULL,
    parent_stage_execution_id binary(16) NULL,
    generated_by_event_id binary(16) NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    version bigint NOT NULL,
    attempt bigint NOT NULL,
    assignee_kind varchar(64) COLLATE utf8mb4_0900_bin NULL,
    assignee_ref varchar(512) COLLATE utf8mb4_0900_bin NULL,
    agent_version_id binary(16) NULL,
    input_contract_json json NOT NULL,
    output_contract_json json NOT NULL,
    policy_json json NOT NULL,
    started_at datetime(6) NULL,
    completed_at datetime(6) NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    CONSTRAINT pk_stage_executions PRIMARY KEY (stage_execution_id),
    CONSTRAINT uq_stages__tenant_id UNIQUE (tenant_id, stage_execution_id),
    CONSTRAINT uq_stages__run_id UNIQUE (tenant_id, run_id, stage_execution_id),
    CONSTRAINT uq_stages__logical_attempt
        UNIQUE (tenant_id, run_id, stage_key, attempt),
    CONSTRAINT fk_stages__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_stages__parent
        FOREIGN KEY (tenant_id, run_id, parent_stage_execution_id)
        REFERENCES stage_executions (tenant_id, run_id, stage_execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_stages__generated_event
        FOREIGN KEY (tenant_id, run_id, generated_by_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT fk_stages__agent_version FOREIGN KEY (tenant_id, agent_version_id)
        REFERENCES agent_definition_versions (tenant_id, agent_version_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_stages__key CHECK (char_length(stage_key) > 0) ENFORCED,
    CONSTRAINT ck_stages__definition_key
        CHECK (definition_stage_key IS NULL OR char_length(definition_stage_key) > 0) ENFORCED,
    CONSTRAINT ck_stages__origin
        CHECK (definition_stage_key IS NOT NULL OR generated_by_event_id IS NOT NULL) ENFORCED,
    CONSTRAINT ck_stages__status CHECK (status IN (
        'planned', 'active', 'waiting_approval', 'rework_required',
        'succeeded', 'failed', 'skipped', 'cancelled'
    )) ENFORCED,
    CONSTRAINT ck_stages__version CHECK (version >= 0) ENFORCED,
    CONSTRAINT ck_stages__attempt CHECK (attempt >= 1) ENFORCED,
    CONSTRAINT ck_stages__assignee CHECK (
        (assignee_kind IS NULL AND assignee_ref IS NULL)
        OR (assignee_kind IS NOT NULL AND char_length(assignee_kind) > 0
            AND assignee_ref IS NOT NULL AND char_length(assignee_ref) > 0)
    ) ENFORCED,
    CONSTRAINT ck_stages__completion CHECK (
        (status IN ('succeeded', 'failed', 'skipped', 'cancelled')
            AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')
            AND completed_at IS NULL)
    ) ENFORCED,
    CONSTRAINT ck_stages__time CHECK (
        updated_at >= created_at
        AND (started_at IS NULL OR started_at >= created_at)
        AND (completed_at IS NULL OR completed_at >= created_at)
    ) ENFORCED,
    INDEX ix_stages__run_status
        (tenant_id, run_id, status, stage_key, attempt),
    INDEX ix_stages__assignee
        (tenant_id, assignee_kind, assignee_ref, status, updated_at, stage_execution_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE tasks (
    task_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    stage_execution_id binary(16) NULL,
    logical_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    kind varchar(64) COLLATE utf8mb4_0900_bin NOT NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    generation bigint NOT NULL,
    based_on_checkpoint_sequence bigint NULL,
    priority integer NOT NULL,
    available_at datetime(6) NOT NULL,
    attempt bigint NOT NULL,
    max_attempts bigint NOT NULL,
    lease_owner binary(16) NULL,
    lease_token binary(32) NULL,
    lease_expires_at datetime(6) NULL,
    input_json json NOT NULL,
    result_json json NULL,
    error_code varchar(128) COLLATE utf8mb4_0900_bin NULL,
    error_json json NULL,
    deadline datetime(6) NULL,
    created_event_id binary(16) NOT NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    completed_at datetime(6) NULL,
    CONSTRAINT pk_tasks PRIMARY KEY (task_id),
    CONSTRAINT uq_tasks__tenant_id UNIQUE (tenant_id, task_id),
    CONSTRAINT uq_tasks__run_id UNIQUE (tenant_id, run_id, task_id),
    CONSTRAINT uq_tasks__logical_generation
        UNIQUE (tenant_id, run_id, logical_key, generation),
    CONSTRAINT fk_tasks__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_tasks__stage FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES stage_executions (tenant_id, run_id, stage_execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_tasks__created_event FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_tasks__logical_key
        CHECK (char_length(logical_key) > 0) ENFORCED,
    CONSTRAINT ck_tasks__kind CHECK (kind IN (
        'model', 'tool', 'agent_server', 'artifact_check', 'timer_wakeup',
        'reconcile', 'stop_external_execution'
    )) ENFORCED,
    CONSTRAINT ck_tasks__status CHECK (status IN (
        'scheduled', 'queued', 'leased', 'retry_scheduled',
        'succeeded', 'failed', 'dead_lettered', 'cancelled'
    )) ENFORCED,
    CONSTRAINT ck_tasks__generation CHECK (generation >= 0) ENFORCED,
    CONSTRAINT ck_tasks__checkpoint_sequence
        CHECK (based_on_checkpoint_sequence IS NULL
            OR based_on_checkpoint_sequence >= 1) ENFORCED,
    CONSTRAINT ck_tasks__attempts
        CHECK (attempt >= 0 AND max_attempts >= 1 AND attempt <= max_attempts) ENFORCED,
    CONSTRAINT ck_tasks__lease CHECK (
        (status = 'leased' AND lease_owner IS NOT NULL
            AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'leased' AND lease_owner IS NULL
            AND lease_token IS NULL AND lease_expires_at IS NULL)
    ) ENFORCED,
    CONSTRAINT ck_tasks__completion CHECK (
        (status IN ('succeeded', 'failed', 'dead_lettered', 'cancelled')
            AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'dead_lettered', 'cancelled')
            AND completed_at IS NULL)
    ) ENFORCED,
    CONSTRAINT ck_tasks__error_code
        CHECK (error_code IS NULL OR char_length(error_code) > 0) ENFORCED,
    CONSTRAINT ck_tasks__deadline
        CHECK (deadline IS NULL OR deadline >= created_at) ENFORCED,
    CONSTRAINT ck_tasks__time CHECK (updated_at >= created_at) ENFORCED,
    INDEX ix_tasks__claim_global
        (status, available_at, priority DESC, task_id),
    INDEX ix_tasks__claim_tenant
        (tenant_id, status, available_at, priority DESC, task_id),
    INDEX ix_tasks__lease_reclaim (status, lease_expires_at, task_id),
    INDEX ix_tasks__run_page (tenant_id, run_id, status, created_at, task_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE task_attempts (
    task_attempt_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    task_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    attempt bigint NOT NULL,
    worker_id binary(16) NOT NULL,
    lease_token_digest binary(32) NOT NULL,
    claimed_at datetime(6) NOT NULL,
    lease_expires_at datetime(6) NOT NULL,
    finished_at datetime(6) NULL,
    outcome varchar(32) COLLATE utf8mb4_0900_bin NULL,
    error_code varchar(128) COLLATE utf8mb4_0900_bin NULL,
    metrics_json json NOT NULL,
    CONSTRAINT pk_task_attempts PRIMARY KEY (task_attempt_id),
    CONSTRAINT uq_task_attempts__tenant_id UNIQUE (tenant_id, task_attempt_id),
    CONSTRAINT uq_task_attempts__number UNIQUE (tenant_id, task_id, attempt),
    CONSTRAINT fk_task_attempts__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_task_attempts__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_task_attempts__attempt CHECK (attempt >= 1) ENFORCED,
    CONSTRAINT ck_task_attempts__outcome CHECK (
        outcome IS NULL OR outcome IN ('succeeded', 'failed', 'lease_expired', 'cancelled')
    ) ENFORCED,
    CONSTRAINT ck_task_attempts__finalization CHECK (
        (finished_at IS NULL AND outcome IS NULL AND error_code IS NULL)
        OR (finished_at IS NOT NULL AND outcome IS NOT NULL)
    ) ENFORCED,
    CONSTRAINT ck_task_attempts__error_code
        CHECK (error_code IS NULL OR char_length(error_code) > 0) ENFORCED,
    CONSTRAINT ck_task_attempts__time CHECK (
        lease_expires_at >= claimed_at
        AND (finished_at IS NULL OR finished_at >= claimed_at)
    ) ENFORCED,
    INDEX ix_task_attempts__run_task (tenant_id, run_id, task_id, attempt)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE checkpoints (
    checkpoint_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    sequence bigint NOT NULL,
    schema_version bigint NOT NULL,
    workflow_version_id binary(16) NULL,
    coordinator_agent_version_id binary(16) NULL,
    execution_generation bigint NOT NULL,
    state_json json NOT NULL,
    state_digest binary(32) NOT NULL,
    created_event_id binary(16) NOT NULL,
    created_at datetime(6) NOT NULL,
    CONSTRAINT pk_checkpoints PRIMARY KEY (checkpoint_id),
    CONSTRAINT uq_checkpoints__tenant_id UNIQUE (tenant_id, checkpoint_id),
    CONSTRAINT uq_checkpoints__run_id UNIQUE (tenant_id, run_id, checkpoint_id),
    CONSTRAINT uq_checkpoints__sequence UNIQUE (tenant_id, run_id, sequence),
    CONSTRAINT fk_checkpoints__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_checkpoints__workflow_version
        FOREIGN KEY (tenant_id, workflow_version_id)
        REFERENCES workflow_definition_versions (tenant_id, workflow_version_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_checkpoints__agent_version
        FOREIGN KEY (tenant_id, coordinator_agent_version_id)
        REFERENCES agent_definition_versions (tenant_id, agent_version_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_checkpoints__created_event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_checkpoints__sequence CHECK (sequence >= 1) ENFORCED,
    CONSTRAINT ck_checkpoints__schema_version CHECK (schema_version >= 1) ENFORCED,
    CONSTRAINT ck_checkpoints__generation CHECK (execution_generation >= 0) ENFORCED,
    INDEX ix_checkpoints__run_time
        (tenant_id, run_id, created_at, checkpoint_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

ALTER TABLE runs
    ADD CONSTRAINT ck_runs__parent_task
    CHECK (parent_task_id IS NULL OR parent_run_id IS NOT NULL) ENFORCED;

ALTER TABLE runs
    ADD CONSTRAINT fk_runs__parent_task
    FOREIGN KEY (tenant_id, parent_run_id, parent_task_id)
    REFERENCES tasks (tenant_id, run_id, task_id)
    ON DELETE RESTRICT;

ALTER TABLE runs
    ADD CONSTRAINT fk_runs__current_checkpoint
    FOREIGN KEY (tenant_id, run_id, current_checkpoint_id)
    REFERENCES checkpoints (tenant_id, run_id, checkpoint_id)
    ON DELETE RESTRICT;
