CREATE TABLE agent_loom.context_snapshots (
    context_snapshot_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    revision bigint NOT NULL,
    parent_context_snapshot_id uuid,
    schema_version integer NOT NULL,
    context_json jsonb NOT NULL,
    context_digest bytea NOT NULL,
    created_event_id uuid NOT NULL,
    created_by varchar(255) COLLATE "C" NOT NULL,
    created_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_context_snapshots PRIMARY KEY (context_snapshot_id),
    CONSTRAINT uq_context_snapshots__tenant_id UNIQUE (tenant_id, context_snapshot_id),
    CONSTRAINT uq_context_snapshots__run_id UNIQUE (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT uq_context_snapshots__revision UNIQUE (tenant_id, run_id, revision),
    CONSTRAINT fk_context_snapshots__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id),
    CONSTRAINT fk_context_snapshots__parent
        FOREIGN KEY (tenant_id, run_id, parent_context_snapshot_id)
        REFERENCES agent_loom.context_snapshots (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT fk_context_snapshots__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id),
    CONSTRAINT ck_context_snapshots__revision CHECK (revision >= 1),
    CONSTRAINT ck_context_snapshots__schema CHECK (schema_version >= 1),
    CONSTRAINT ck_context_snapshots__digest CHECK (octet_length(context_digest) = 32),
    CONSTRAINT ck_context_snapshots__creator CHECK (length(created_by) > 0),
    CONSTRAINT ck_context_snapshots__parent CHECK (
        (revision = 1 AND parent_context_snapshot_id IS NULL)
        OR (revision > 1 AND parent_context_snapshot_id IS NOT NULL)
    )
);

CREATE INDEX ix_context_snapshots__history
    ON agent_loom.context_snapshots (tenant_id, run_id, revision, context_snapshot_id);

CREATE TABLE agent_loom.context_patches (
    context_patch_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    base_context_snapshot_id uuid NOT NULL,
    result_context_snapshot_id uuid NOT NULL,
    schema_version integer NOT NULL,
    merge_strategy varchar(32) COLLATE "C" NOT NULL,
    patch_json jsonb NOT NULL,
    created_event_id uuid NOT NULL,
    created_by varchar(255) COLLATE "C" NOT NULL,
    created_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_context_patches PRIMARY KEY (context_patch_id),
    CONSTRAINT uq_context_patches__tenant_id UNIQUE (tenant_id, context_patch_id),
    CONSTRAINT uq_context_patches__result UNIQUE (tenant_id, run_id, result_context_snapshot_id),
    CONSTRAINT fk_context_patches__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id),
    CONSTRAINT fk_context_patches__base
        FOREIGN KEY (tenant_id, run_id, base_context_snapshot_id)
        REFERENCES agent_loom.context_snapshots (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT fk_context_patches__result
        FOREIGN KEY (tenant_id, run_id, result_context_snapshot_id)
        REFERENCES agent_loom.context_snapshots (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT fk_context_patches__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id),
    CONSTRAINT ck_context_patches__schema CHECK (schema_version >= 1),
    CONSTRAINT ck_context_patches__strategy CHECK (merge_strategy IN ('replace', 'merge_patch')),
    CONSTRAINT ck_context_patches__creator CHECK (length(created_by) > 0)
);

INSERT INTO agent_loom.context_snapshots (
    context_snapshot_id, tenant_id, run_id, revision, parent_context_snapshot_id,
    schema_version, context_json, context_digest, created_event_id, created_by, created_at
)
SELECT r.run_id, r.tenant_id, r.run_id, 1, NULL, 1, r.input_json,
       decode(repeat('00', 32), 'hex'), e.event_id,
       'migration/0017_context_snapshots', r.created_at
FROM agent_loom.runs r
JOIN LATERAL (
    SELECT event_id FROM agent_loom.events
    WHERE tenant_id = r.tenant_id AND run_id = r.run_id
    ORDER BY sequence LIMIT 1
) e ON true;

ALTER TABLE agent_loom.runs
    ADD COLUMN current_context_snapshot_id uuid,
    ADD COLUMN current_context_revision bigint NOT NULL DEFAULT 0;

UPDATE agent_loom.runs
SET current_context_snapshot_id = run_id, current_context_revision = 1;

ALTER TABLE agent_loom.runs
    ADD CONSTRAINT ck_runs__context_revision CHECK (
        (current_context_revision = 0 AND current_context_snapshot_id IS NULL)
        OR (current_context_revision >= 1 AND current_context_snapshot_id IS NOT NULL)
    ),
    ADD CONSTRAINT fk_runs__current_context_snapshot
        FOREIGN KEY (tenant_id, run_id, current_context_snapshot_id)
        REFERENCES agent_loom.context_snapshots (tenant_id, run_id, context_snapshot_id);
