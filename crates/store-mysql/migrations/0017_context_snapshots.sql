CREATE TABLE context_snapshots (
    context_snapshot_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    revision bigint NOT NULL,
    parent_context_snapshot_id binary(16) NULL,
    schema_version integer NOT NULL,
    context_json json NOT NULL,
    context_digest binary(32) NOT NULL,
    created_event_id binary(16) NOT NULL,
    created_by varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    created_at datetime(6) NOT NULL,
    CONSTRAINT pk_context_snapshots PRIMARY KEY (context_snapshot_id),
    CONSTRAINT uq_context_snapshots__tenant_id UNIQUE (tenant_id, context_snapshot_id),
    CONSTRAINT uq_context_snapshots__run_id UNIQUE (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT uq_context_snapshots__revision UNIQUE (tenant_id, run_id, revision),
    CONSTRAINT fk_context_snapshots__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id),
    CONSTRAINT fk_context_snapshots__parent
        FOREIGN KEY (tenant_id, run_id, parent_context_snapshot_id)
        REFERENCES context_snapshots (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT fk_context_snapshots__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id),
    CONSTRAINT ck_context_snapshots__revision CHECK (revision >= 1) ENFORCED,
    CONSTRAINT ck_context_snapshots__schema CHECK (schema_version >= 1) ENFORCED,
    CONSTRAINT ck_context_snapshots__creator CHECK (char_length(created_by) > 0) ENFORCED,
    CONSTRAINT ck_context_snapshots__parent CHECK (
        (revision = 1 AND parent_context_snapshot_id IS NULL)
        OR (revision > 1 AND parent_context_snapshot_id IS NOT NULL)
    ) ENFORCED
) ENGINE=InnoDB;

CREATE INDEX ix_context_snapshots__history
    ON context_snapshots (tenant_id, run_id, revision, context_snapshot_id);

CREATE TABLE context_patches (
    context_patch_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    base_context_snapshot_id binary(16) NOT NULL,
    result_context_snapshot_id binary(16) NOT NULL,
    schema_version integer NOT NULL,
    merge_strategy varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    patch_json json NOT NULL,
    created_event_id binary(16) NOT NULL,
    created_by varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    created_at datetime(6) NOT NULL,
    CONSTRAINT pk_context_patches PRIMARY KEY (context_patch_id),
    CONSTRAINT uq_context_patches__tenant_id UNIQUE (tenant_id, context_patch_id),
    CONSTRAINT uq_context_patches__result UNIQUE (tenant_id, run_id, result_context_snapshot_id),
    CONSTRAINT fk_context_patches__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id),
    CONSTRAINT fk_context_patches__base
        FOREIGN KEY (tenant_id, run_id, base_context_snapshot_id)
        REFERENCES context_snapshots (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT fk_context_patches__result
        FOREIGN KEY (tenant_id, run_id, result_context_snapshot_id)
        REFERENCES context_snapshots (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT fk_context_patches__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id),
    CONSTRAINT ck_context_patches__schema CHECK (schema_version >= 1) ENFORCED,
    CONSTRAINT ck_context_patches__strategy CHECK (merge_strategy IN ('replace', 'merge_patch')) ENFORCED,
    CONSTRAINT ck_context_patches__creator CHECK (char_length(created_by) > 0) ENFORCED
) ENGINE=InnoDB;

INSERT INTO context_snapshots (
    context_snapshot_id, tenant_id, run_id, revision, parent_context_snapshot_id,
    schema_version, context_json, context_digest, created_event_id, created_by, created_at
)
SELECT r.run_id, r.tenant_id, r.run_id, 1, NULL, 1, r.input_json,
       UNHEX(REPEAT('00', 32)),
       (SELECT e.event_id FROM events e
        WHERE e.tenant_id = r.tenant_id AND e.run_id = r.run_id
        ORDER BY e.sequence LIMIT 1),
       'migration/0017_context_snapshots', r.created_at
FROM runs r;

ALTER TABLE runs
    ADD COLUMN current_context_snapshot_id binary(16) NULL,
    ADD COLUMN current_context_revision bigint NOT NULL DEFAULT 0;

UPDATE runs
SET current_context_snapshot_id = run_id, current_context_revision = 1;

ALTER TABLE runs
    ADD CONSTRAINT ck_runs__context_revision CHECK (
        (current_context_revision = 0 AND current_context_snapshot_id IS NULL)
        OR (current_context_revision >= 1 AND current_context_snapshot_id IS NOT NULL)
    ) ENFORCED,
    ADD CONSTRAINT fk_runs__current_context_snapshot
        FOREIGN KEY (tenant_id, run_id, current_context_snapshot_id)
        REFERENCES context_snapshots (tenant_id, run_id, context_snapshot_id);
