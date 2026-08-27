CREATE TABLE plan_revisions (
    plan_revision_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    revision bigint NOT NULL,
    parent_plan_revision_id binary(16) NULL,
    schema_version bigint NOT NULL,
    plan_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    plan_json json NOT NULL,
    plan_digest binary(32) NOT NULL,
    change_summary_json json NOT NULL,
    created_event_id binary(16) NOT NULL,
    created_by varchar(512) COLLATE utf8mb4_0900_bin NOT NULL,
    created_at datetime(6) NOT NULL,
    CONSTRAINT pk_plan_revisions PRIMARY KEY (plan_revision_id),
    CONSTRAINT uq_plan_revisions__tenant_id UNIQUE (tenant_id, plan_revision_id),
    CONSTRAINT uq_plan_revisions__run_id UNIQUE (tenant_id, run_id, plan_revision_id),
    CONSTRAINT uq_plan_revisions__revision UNIQUE (tenant_id, run_id, revision),
    CONSTRAINT fk_plan_revisions__run FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT fk_plan_revisions__parent
        FOREIGN KEY (tenant_id, run_id, parent_plan_revision_id)
        REFERENCES plan_revisions (tenant_id, run_id, plan_revision_id) ON DELETE RESTRICT,
    CONSTRAINT fk_plan_revisions__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_plan_revisions__revision CHECK (revision >= 1) ENFORCED,
    CONSTRAINT ck_plan_revisions__schema CHECK (schema_version >= 1) ENFORCED,
    CONSTRAINT ck_plan_revisions__key CHECK (char_length(plan_key) > 0) ENFORCED,
    CONSTRAINT ck_plan_revisions__creator CHECK (char_length(created_by) > 0) ENFORCED,
    CONSTRAINT ck_plan_revisions__parent CHECK (
        (revision = 1 AND parent_plan_revision_id IS NULL)
        OR (revision > 1 AND parent_plan_revision_id IS NOT NULL)
    ) ENFORCED,
    INDEX ix_plan_revisions__run_history (tenant_id, run_id, revision, plan_revision_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

INSERT INTO plan_revisions (
    plan_revision_id, tenant_id, run_id, revision, parent_plan_revision_id,
    schema_version, plan_key, plan_json, plan_digest, change_summary_json,
    created_event_id, created_by, created_at
)
SELECT r.run_id, r.tenant_id, r.run_id, 1, NULL, 1,
       COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(v.spec_json, '$.plan_key')), ''), 'legacy'),
       COALESCE(v.spec_json, JSON_OBJECT()),
       COALESCE(v.spec_digest,
           UNHEX('44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a')),
       JSON_OBJECT('kind', 'migration_backfill', 'migration', '0015_plan_revisions'),
       e.event_id, 'migration/0015_plan_revisions', r.created_at
FROM runs r
JOIN events e
  ON e.tenant_id = r.tenant_id AND e.run_id = r.run_id AND e.sequence = 1
LEFT JOIN workflow_definition_versions v
  ON v.tenant_id = r.tenant_id AND v.workflow_version_id = r.workflow_version_id;

ALTER TABLE runs
    ADD COLUMN current_plan_revision_id binary(16) NULL,
    ADD COLUMN current_plan_revision bigint NOT NULL DEFAULT 0;

UPDATE runs
SET current_plan_revision_id = run_id, current_plan_revision = 1;

ALTER TABLE runs
    ADD CONSTRAINT ck_runs__plan_revision CHECK (
        (current_plan_revision = 0 AND current_plan_revision_id IS NULL)
        OR (current_plan_revision >= 1 AND current_plan_revision_id IS NOT NULL)
    ) ENFORCED,
    ADD CONSTRAINT fk_runs__current_plan_revision
        FOREIGN KEY (tenant_id, run_id, current_plan_revision_id)
        REFERENCES plan_revisions (tenant_id, run_id, plan_revision_id) ON DELETE RESTRICT;
