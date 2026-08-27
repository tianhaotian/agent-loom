CREATE TABLE agent_loom.plan_revisions (
    plan_revision_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    revision bigint NOT NULL,
    parent_plan_revision_id uuid,
    schema_version bigint NOT NULL,
    plan_key varchar(255) COLLATE "C" NOT NULL,
    plan_json jsonb NOT NULL,
    plan_digest bytea NOT NULL,
    change_summary_json jsonb NOT NULL,
    created_event_id uuid NOT NULL,
    created_by varchar(512) COLLATE "C" NOT NULL,
    created_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_plan_revisions PRIMARY KEY (plan_revision_id),
    CONSTRAINT uq_plan_revisions__tenant_id UNIQUE (tenant_id, plan_revision_id),
    CONSTRAINT uq_plan_revisions__run_id UNIQUE (tenant_id, run_id, plan_revision_id),
    CONSTRAINT uq_plan_revisions__revision UNIQUE (tenant_id, run_id, revision),
    CONSTRAINT fk_plan_revisions__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_plan_revisions__parent
        FOREIGN KEY (tenant_id, run_id, parent_plan_revision_id)
        REFERENCES agent_loom.plan_revisions (tenant_id, run_id, plan_revision_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_plan_revisions__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_plan_revisions__revision CHECK (revision >= 1),
    CONSTRAINT ck_plan_revisions__schema CHECK (schema_version >= 1),
    CONSTRAINT ck_plan_revisions__key CHECK (length(plan_key) > 0),
    CONSTRAINT ck_plan_revisions__digest CHECK (octet_length(plan_digest) = 32),
    CONSTRAINT ck_plan_revisions__creator CHECK (length(created_by) > 0),
    CONSTRAINT ck_plan_revisions__parent CHECK (
        (revision = 1 AND parent_plan_revision_id IS NULL)
        OR (revision > 1 AND parent_plan_revision_id IS NOT NULL)
    )
);

CREATE INDEX ix_plan_revisions__run_history
    ON agent_loom.plan_revisions (tenant_id, run_id, revision, plan_revision_id);

INSERT INTO agent_loom.plan_revisions (
    plan_revision_id, tenant_id, run_id, revision, parent_plan_revision_id,
    schema_version, plan_key, plan_json, plan_digest, change_summary_json,
    created_event_id, created_by, created_at
)
SELECT r.run_id, r.tenant_id, r.run_id, 1, NULL, 1,
       COALESCE(NULLIF(v.spec_json ->> 'plan_key', ''), 'legacy'),
       COALESCE(v.spec_json, '{}'::jsonb),
       COALESCE(v.spec_digest,
           decode('44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', 'hex')),
       '{"kind":"migration_backfill","migration":"0015_plan_revisions"}'::jsonb,
       e.event_id, 'migration/0015_plan_revisions', r.created_at
FROM agent_loom.runs r
JOIN agent_loom.events e
  ON e.tenant_id = r.tenant_id AND e.run_id = r.run_id AND e.sequence = 1
LEFT JOIN agent_loom.workflow_definition_versions v
  ON v.tenant_id = r.tenant_id AND v.workflow_version_id = r.workflow_version_id;

ALTER TABLE agent_loom.runs
    ADD COLUMN current_plan_revision_id uuid,
    ADD COLUMN current_plan_revision bigint NOT NULL DEFAULT 0;

UPDATE agent_loom.runs
SET current_plan_revision_id = run_id, current_plan_revision = 1;

ALTER TABLE agent_loom.runs
    ADD CONSTRAINT ck_runs__plan_revision CHECK (
        (current_plan_revision = 0 AND current_plan_revision_id IS NULL)
        OR (current_plan_revision >= 1 AND current_plan_revision_id IS NOT NULL)
    ),
    ADD CONSTRAINT fk_runs__current_plan_revision
        FOREIGN KEY (tenant_id, run_id, current_plan_revision_id)
        REFERENCES agent_loom.plan_revisions (tenant_id, run_id, plan_revision_id)
        ON DELETE RESTRICT;
