CREATE TABLE agent_loom.stage_executions (
    stage_execution_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    stage_key varchar(255) COLLATE "C" NOT NULL,
    definition_stage_key varchar(255) COLLATE "C",
    parent_stage_execution_id uuid,
    generated_by_event_id uuid,
    status varchar(32) COLLATE "C" NOT NULL,
    version bigint NOT NULL,
    attempt bigint NOT NULL,
    assignee_kind varchar(64) COLLATE "C",
    assignee_ref varchar(512) COLLATE "C",
    agent_version_id uuid,
    input_contract_json jsonb NOT NULL,
    output_contract_json jsonb NOT NULL,
    policy_json jsonb NOT NULL,
    started_at timestamptz(6),
    completed_at timestamptz(6),
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_stage_executions PRIMARY KEY (stage_execution_id),
    CONSTRAINT uq_stages__tenant_id UNIQUE (tenant_id, stage_execution_id),
    CONSTRAINT uq_stages__run_id UNIQUE (tenant_id, run_id, stage_execution_id),
    CONSTRAINT uq_stages__logical_attempt
        UNIQUE (tenant_id, run_id, stage_key, attempt),
    CONSTRAINT fk_stages__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_stages__parent
        FOREIGN KEY (tenant_id, run_id, parent_stage_execution_id)
        REFERENCES agent_loom.stage_executions
            (tenant_id, run_id, stage_execution_id) ON DELETE RESTRICT,
    CONSTRAINT fk_stages__generated_event
        FOREIGN KEY (tenant_id, run_id, generated_by_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_stages__agent_version FOREIGN KEY (tenant_id, agent_version_id)
        REFERENCES agent_loom.agent_definition_versions (tenant_id, agent_version_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_stages__key CHECK (length(stage_key) > 0),
    CONSTRAINT ck_stages__definition_key
        CHECK (definition_stage_key IS NULL OR length(definition_stage_key) > 0),
    CONSTRAINT ck_stages__origin
        CHECK (definition_stage_key IS NOT NULL OR generated_by_event_id IS NOT NULL),
    CONSTRAINT ck_stages__status CHECK (status IN (
        'planned', 'active', 'waiting_approval', 'rework_required',
        'succeeded', 'failed', 'skipped', 'cancelled'
    )),
    CONSTRAINT ck_stages__version CHECK (version >= 0),
    CONSTRAINT ck_stages__attempt CHECK (attempt >= 1),
    CONSTRAINT ck_stages__assignee CHECK (
        (assignee_kind IS NULL AND assignee_ref IS NULL)
        OR (assignee_kind IS NOT NULL AND length(assignee_kind) > 0
            AND assignee_ref IS NOT NULL AND length(assignee_ref) > 0)
    ),
    CONSTRAINT ck_stages__completion CHECK (
        (status IN ('succeeded', 'failed', 'skipped', 'cancelled')
            AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')
            AND completed_at IS NULL)
    ),
    CONSTRAINT ck_stages__time CHECK (
        updated_at >= created_at
        AND (started_at IS NULL OR started_at >= created_at)
        AND (completed_at IS NULL OR completed_at >= created_at)
    )
);

CREATE INDEX ix_stages__run_status
    ON agent_loom.stage_executions
        (tenant_id, run_id, status, stage_key, attempt);
CREATE INDEX ix_stages__assignee
    ON agent_loom.stage_executions
        (tenant_id, assignee_kind, assignee_ref, status, updated_at, stage_execution_id);

CREATE TABLE agent_loom.tasks (
    task_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    stage_execution_id uuid,
    logical_key varchar(255) COLLATE "C" NOT NULL,
    kind varchar(64) COLLATE "C" NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    generation bigint NOT NULL,
    based_on_checkpoint_sequence bigint,
    priority integer NOT NULL,
    available_at timestamptz(6) NOT NULL,
    attempt bigint NOT NULL,
    max_attempts bigint NOT NULL,
    lease_owner uuid,
    lease_token bytea,
    lease_expires_at timestamptz(6),
    input_json jsonb NOT NULL,
    result_json jsonb,
    error_code varchar(128) COLLATE "C",
    error_json jsonb,
    deadline timestamptz(6),
    created_event_id uuid NOT NULL,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    completed_at timestamptz(6),
    CONSTRAINT pk_tasks PRIMARY KEY (task_id),
    CONSTRAINT uq_tasks__tenant_id UNIQUE (tenant_id, task_id),
    CONSTRAINT uq_tasks__run_id UNIQUE (tenant_id, run_id, task_id),
    CONSTRAINT uq_tasks__logical_generation
        UNIQUE (tenant_id, run_id, logical_key, generation),
    CONSTRAINT fk_tasks__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_tasks__stage FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES agent_loom.stage_executions
            (tenant_id, run_id, stage_execution_id) ON DELETE RESTRICT,
    CONSTRAINT fk_tasks__created_event FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_tasks__logical_key CHECK (length(logical_key) > 0),
    CONSTRAINT ck_tasks__kind CHECK (kind IN (
        'model', 'tool', 'agent_server', 'artifact_check', 'timer_wakeup',
        'reconcile', 'stop_external_execution'
    )),
    CONSTRAINT ck_tasks__status CHECK (status IN (
        'scheduled', 'queued', 'leased', 'retry_scheduled',
        'succeeded', 'failed', 'dead_lettered', 'cancelled'
    )),
    CONSTRAINT ck_tasks__generation CHECK (generation >= 0),
    CONSTRAINT ck_tasks__checkpoint_sequence
        CHECK (based_on_checkpoint_sequence IS NULL OR based_on_checkpoint_sequence >= 1),
    CONSTRAINT ck_tasks__attempts
        CHECK (attempt >= 0 AND max_attempts >= 1 AND attempt <= max_attempts),
    CONSTRAINT ck_tasks__lease CHECK (
        (status = 'leased' AND lease_owner IS NOT NULL
            AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'leased' AND lease_owner IS NULL
            AND lease_token IS NULL AND lease_expires_at IS NULL)
    ),
    CONSTRAINT ck_tasks__lease_token
        CHECK (lease_token IS NULL OR octet_length(lease_token) = 32),
    CONSTRAINT ck_tasks__completion CHECK (
        (status IN ('succeeded', 'failed', 'dead_lettered', 'cancelled')
            AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'dead_lettered', 'cancelled')
            AND completed_at IS NULL)
    ),
    CONSTRAINT ck_tasks__error_code
        CHECK (error_code IS NULL OR length(error_code) > 0),
    CONSTRAINT ck_tasks__deadline CHECK (deadline IS NULL OR deadline >= created_at),
    CONSTRAINT ck_tasks__time CHECK (updated_at >= created_at)
);

CREATE INDEX ix_tasks__claim_global
    ON agent_loom.tasks (status, available_at, priority DESC, task_id);
CREATE INDEX ix_tasks__claim_tenant
    ON agent_loom.tasks (tenant_id, status, available_at, priority DESC, task_id);
CREATE INDEX ix_tasks__lease_reclaim
    ON agent_loom.tasks (status, lease_expires_at, task_id);
CREATE INDEX ix_tasks__run_page
    ON agent_loom.tasks (tenant_id, run_id, status, created_at, task_id);

CREATE TABLE agent_loom.task_attempts (
    task_attempt_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    task_id uuid NOT NULL,
    run_id uuid NOT NULL,
    attempt bigint NOT NULL,
    worker_id uuid NOT NULL,
    lease_token_digest bytea NOT NULL,
    claimed_at timestamptz(6) NOT NULL,
    lease_expires_at timestamptz(6) NOT NULL,
    finished_at timestamptz(6),
    outcome varchar(32) COLLATE "C",
    error_code varchar(128) COLLATE "C",
    metrics_json jsonb NOT NULL,
    CONSTRAINT pk_task_attempts PRIMARY KEY (task_attempt_id),
    CONSTRAINT uq_task_attempts__tenant_id UNIQUE (tenant_id, task_attempt_id),
    CONSTRAINT uq_task_attempts__number UNIQUE (tenant_id, task_id, attempt),
    CONSTRAINT fk_task_attempts__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES agent_loom.tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_task_attempts__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_task_attempts__attempt CHECK (attempt >= 1),
    CONSTRAINT ck_task_attempts__lease_digest
        CHECK (octet_length(lease_token_digest) = 32),
    CONSTRAINT ck_task_attempts__outcome CHECK (
        outcome IS NULL OR outcome IN ('succeeded', 'failed', 'lease_expired', 'cancelled')
    ),
    CONSTRAINT ck_task_attempts__finalization CHECK (
        (finished_at IS NULL AND outcome IS NULL AND error_code IS NULL)
        OR (finished_at IS NOT NULL AND outcome IS NOT NULL)
    ),
    CONSTRAINT ck_task_attempts__error_code
        CHECK (error_code IS NULL OR length(error_code) > 0),
    CONSTRAINT ck_task_attempts__time CHECK (
        lease_expires_at >= claimed_at
        AND (finished_at IS NULL OR finished_at >= claimed_at)
    )
);

CREATE INDEX ix_task_attempts__run_task
    ON agent_loom.task_attempts (tenant_id, run_id, task_id, attempt);

CREATE TABLE agent_loom.checkpoints (
    checkpoint_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    sequence bigint NOT NULL,
    schema_version bigint NOT NULL,
    workflow_version_id uuid,
    coordinator_agent_version_id uuid,
    execution_generation bigint NOT NULL,
    state_json jsonb NOT NULL,
    state_digest bytea NOT NULL,
    created_event_id uuid NOT NULL,
    created_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_checkpoints PRIMARY KEY (checkpoint_id),
    CONSTRAINT uq_checkpoints__tenant_id UNIQUE (tenant_id, checkpoint_id),
    CONSTRAINT uq_checkpoints__run_id UNIQUE (tenant_id, run_id, checkpoint_id),
    CONSTRAINT uq_checkpoints__sequence UNIQUE (tenant_id, run_id, sequence),
    CONSTRAINT fk_checkpoints__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_checkpoints__workflow_version
        FOREIGN KEY (tenant_id, workflow_version_id)
        REFERENCES agent_loom.workflow_definition_versions
            (tenant_id, workflow_version_id) ON DELETE RESTRICT,
    CONSTRAINT fk_checkpoints__agent_version
        FOREIGN KEY (tenant_id, coordinator_agent_version_id)
        REFERENCES agent_loom.agent_definition_versions
            (tenant_id, agent_version_id) ON DELETE RESTRICT,
    CONSTRAINT fk_checkpoints__created_event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_checkpoints__sequence CHECK (sequence >= 1),
    CONSTRAINT ck_checkpoints__schema_version CHECK (schema_version >= 1),
    CONSTRAINT ck_checkpoints__generation CHECK (execution_generation >= 0),
    CONSTRAINT ck_checkpoints__digest CHECK (octet_length(state_digest) = 32)
);

CREATE INDEX ix_checkpoints__run_time
    ON agent_loom.checkpoints (tenant_id, run_id, created_at, checkpoint_id);

ALTER TABLE agent_loom.runs
    ADD CONSTRAINT ck_runs__parent_task
    CHECK (parent_task_id IS NULL OR parent_run_id IS NOT NULL);

ALTER TABLE agent_loom.runs
    ADD CONSTRAINT fk_runs__parent_task
    FOREIGN KEY (tenant_id, parent_run_id, parent_task_id)
    REFERENCES agent_loom.tasks (tenant_id, run_id, task_id)
    ON DELETE RESTRICT;

ALTER TABLE agent_loom.runs
    ADD CONSTRAINT fk_runs__current_checkpoint
    FOREIGN KEY (tenant_id, run_id, current_checkpoint_id)
    REFERENCES agent_loom.checkpoints (tenant_id, run_id, checkpoint_id)
    ON DELETE RESTRICT;
