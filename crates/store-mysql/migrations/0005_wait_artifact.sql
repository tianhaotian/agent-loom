CREATE TABLE wait_subscriptions (
    wait_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    stage_execution_id binary(16) NULL,
    wait_type varchar(64) COLLATE utf8mb4_0900_bin NOT NULL,
    expected_event_type varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    match_key_hash binary(32) NOT NULL,
    match_contract_json json NOT NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    active_slot tinyint NULL,
    expires_at datetime(6) NULL,
    consumed_by_event_id binary(16) NULL,
    created_event_id binary(16) NOT NULL,
    created_at datetime(6) NOT NULL,
    consumed_at datetime(6) NULL,
    updated_at datetime(6) NOT NULL,
    CONSTRAINT pk_wait_subscriptions PRIMARY KEY (wait_id),
    CONSTRAINT uq_waits__tenant_id UNIQUE (tenant_id, wait_id),
    CONSTRAINT uq_waits__active_slot UNIQUE (
        tenant_id, run_id, wait_type, expected_event_type, match_key_hash, active_slot
    ),
    CONSTRAINT fk_waits__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_waits__stage FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES stage_executions (tenant_id, run_id, stage_execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_waits__consumed_event
        FOREIGN KEY (tenant_id, run_id, consumed_by_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT fk_waits__created_event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_waits__type CHECK (char_length(wait_type) > 0) ENFORCED,
    CONSTRAINT ck_waits__event_type
        CHECK (char_length(expected_event_type) > 0) ENFORCED,
    CONSTRAINT ck_waits__status
        CHECK (status IN ('open', 'consumed', 'expired', 'cancelled')) ENFORCED,
    CONSTRAINT ck_waits__active_state CHECK (
        (status = 'open' AND active_slot = 1
            AND consumed_by_event_id IS NULL AND consumed_at IS NULL)
        OR
        (status = 'consumed' AND active_slot IS NULL
            AND consumed_by_event_id IS NOT NULL AND consumed_at IS NOT NULL)
        OR
        (status IN ('expired', 'cancelled') AND active_slot IS NULL
            AND consumed_by_event_id IS NULL AND consumed_at IS NULL)
    ) ENFORCED,
    CONSTRAINT ck_waits__time CHECK (
        updated_at >= created_at
        AND (expires_at IS NULL OR expires_at >= created_at)
        AND (consumed_at IS NULL OR consumed_at >= created_at)
    ) ENFORCED,
    INDEX ix_waits__event_match
        (tenant_id, status, expected_event_type, match_key_hash, wait_id),
    INDEX ix_waits__expiry (status, expires_at, wait_id),
    INDEX ix_waits__run_page (tenant_id, run_id, status, created_at, wait_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE artifact_refs (
    artifact_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    stage_execution_id binary(16) NULL,
    task_id binary(16) NULL,
    logical_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    kind varchar(128) COLLATE utf8mb4_0900_bin NOT NULL,
    contract_version bigint NOT NULL,
    version bigint NOT NULL,
    uri text NOT NULL,
    digest binary(32) NOT NULL,
    media_type varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    size_bytes bigint NOT NULL,
    source_artifact_refs_json json NOT NULL,
    metadata_json json NOT NULL,
    produced_by varchar(512) COLLATE utf8mb4_0900_bin NOT NULL,
    created_event_id binary(16) NOT NULL,
    created_at datetime(6) NOT NULL,
    CONSTRAINT pk_artifact_refs PRIMARY KEY (artifact_id),
    CONSTRAINT uq_artifacts__tenant_id UNIQUE (tenant_id, artifact_id),
    CONSTRAINT uq_artifacts__logical_version
        UNIQUE (tenant_id, run_id, logical_key, version),
    CONSTRAINT fk_artifacts__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_artifacts__stage
        FOREIGN KEY (tenant_id, run_id, stage_execution_id)
        REFERENCES stage_executions (tenant_id, run_id, stage_execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_artifacts__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_artifacts__created_event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_artifacts__logical_key
        CHECK (char_length(logical_key) > 0) ENFORCED,
    CONSTRAINT ck_artifacts__kind CHECK (char_length(kind) > 0) ENFORCED,
    CONSTRAINT ck_artifacts__contract_version
        CHECK (contract_version >= 1) ENFORCED,
    CONSTRAINT ck_artifacts__version CHECK (version >= 1) ENFORCED,
    CONSTRAINT ck_artifacts__uri CHECK (char_length(uri) > 0) ENFORCED,
    CONSTRAINT ck_artifacts__media_type
        CHECK (char_length(media_type) > 0) ENFORCED,
    CONSTRAINT ck_artifacts__size CHECK (size_bytes >= 0) ENFORCED,
    CONSTRAINT ck_artifacts__producer
        CHECK (char_length(produced_by) > 0) ENFORCED,
    INDEX ix_artifacts__stage_kind
        (tenant_id, run_id, stage_execution_id, kind, version, artifact_id),
    INDEX ix_artifacts__digest (tenant_id, digest, artifact_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;
