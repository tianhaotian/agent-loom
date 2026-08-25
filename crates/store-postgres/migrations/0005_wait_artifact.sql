CREATE TABLE agent_loom.wait_subscriptions (
    wait_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    stage_execution_id uuid,
    wait_type varchar(64) COLLATE "C" NOT NULL,
    expected_event_type varchar(255) COLLATE "C" NOT NULL,
    match_key_hash bytea NOT NULL,
    match_contract_json jsonb NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    active_slot smallint,
    expires_at timestamptz(6),
    consumed_by_event_id uuid,
    created_event_id uuid NOT NULL,
    created_at timestamptz(6) NOT NULL,
    consumed_at timestamptz(6),
    updated_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_wait_subscriptions PRIMARY KEY (wait_id),
    CONSTRAINT uq_waits__tenant_id UNIQUE (tenant_id, wait_id),
    CONSTRAINT uq_waits__active_slot UNIQUE (
        tenant_id, run_id, wait_type, expected_event_type, match_key_hash, active_slot
    ),
    CONSTRAINT fk_waits__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_waits__stage FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES agent_loom.stage_executions
            (tenant_id, run_id, stage_execution_id) ON DELETE RESTRICT,
    CONSTRAINT fk_waits__consumed_event
        FOREIGN KEY (tenant_id, run_id, consumed_by_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_waits__created_event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_waits__type CHECK (length(wait_type) > 0),
    CONSTRAINT ck_waits__event_type CHECK (length(expected_event_type) > 0),
    CONSTRAINT ck_waits__match_hash CHECK (octet_length(match_key_hash) = 32),
    CONSTRAINT ck_waits__status
        CHECK (status IN ('open', 'consumed', 'expired', 'cancelled')),
    CONSTRAINT ck_waits__active_state CHECK (
        (status = 'open' AND active_slot = 1
            AND consumed_by_event_id IS NULL AND consumed_at IS NULL)
        OR
        (status = 'consumed' AND active_slot IS NULL
            AND consumed_by_event_id IS NOT NULL AND consumed_at IS NOT NULL)
        OR
        (status IN ('expired', 'cancelled') AND active_slot IS NULL
            AND consumed_by_event_id IS NULL AND consumed_at IS NULL)
    ),
    CONSTRAINT ck_waits__time CHECK (
        updated_at >= created_at
        AND (expires_at IS NULL OR expires_at >= created_at)
        AND (consumed_at IS NULL OR consumed_at >= created_at)
    )
);

CREATE INDEX ix_waits__event_match
    ON agent_loom.wait_subscriptions
        (tenant_id, status, expected_event_type, match_key_hash, wait_id);
CREATE INDEX ix_waits__expiry
    ON agent_loom.wait_subscriptions (status, expires_at, wait_id);
CREATE INDEX ix_waits__run_page
    ON agent_loom.wait_subscriptions (tenant_id, run_id, status, created_at, wait_id);

CREATE TABLE agent_loom.artifact_refs (
    artifact_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    stage_execution_id uuid,
    task_id uuid,
    logical_key varchar(255) COLLATE "C" NOT NULL,
    kind varchar(128) COLLATE "C" NOT NULL,
    contract_version bigint NOT NULL,
    version bigint NOT NULL,
    uri text NOT NULL,
    digest bytea NOT NULL,
    media_type varchar(255) COLLATE "C" NOT NULL,
    size_bytes bigint NOT NULL,
    source_artifact_refs_json jsonb NOT NULL,
    metadata_json jsonb NOT NULL,
    produced_by varchar(512) COLLATE "C" NOT NULL,
    created_event_id uuid NOT NULL,
    created_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_artifact_refs PRIMARY KEY (artifact_id),
    CONSTRAINT uq_artifacts__tenant_id UNIQUE (tenant_id, artifact_id),
    CONSTRAINT uq_artifacts__logical_version
        UNIQUE (tenant_id, run_id, logical_key, version),
    CONSTRAINT fk_artifacts__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_artifacts__stage
        FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES agent_loom.stage_executions
            (tenant_id, run_id, stage_execution_id) ON DELETE RESTRICT,
    CONSTRAINT fk_artifacts__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES agent_loom.tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_artifacts__created_event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_artifacts__logical_key CHECK (length(logical_key) > 0),
    CONSTRAINT ck_artifacts__kind CHECK (length(kind) > 0),
    CONSTRAINT ck_artifacts__contract_version CHECK (contract_version >= 1),
    CONSTRAINT ck_artifacts__version CHECK (version >= 1),
    CONSTRAINT ck_artifacts__uri CHECK (length(uri) > 0),
    CONSTRAINT ck_artifacts__digest CHECK (octet_length(digest) = 32),
    CONSTRAINT ck_artifacts__media_type CHECK (length(media_type) > 0),
    CONSTRAINT ck_artifacts__size CHECK (size_bytes >= 0),
    CONSTRAINT ck_artifacts__producer CHECK (length(produced_by) > 0)
);

CREATE INDEX ix_artifacts__stage_kind
    ON agent_loom.artifact_refs
        (tenant_id, run_id, stage_execution_id, kind, version, artifact_id);
CREATE INDEX ix_artifacts__digest
    ON agent_loom.artifact_refs (tenant_id, digest, artifact_id);
