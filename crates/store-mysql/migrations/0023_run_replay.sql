ALTER TABLE runs
    ADD COLUMN replay_of_run_id binary(16),
    ADD CONSTRAINT fk_runs__replay_source
        FOREIGN KEY (tenant_id, replay_of_run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE RESTRICT,
    ADD CONSTRAINT ck_runs__replay_not_self
        CHECK (replay_of_run_id IS NULL OR replay_of_run_id <> run_id) ENFORCED,
    ADD INDEX ix_runs__replay_source
        (tenant_id, replay_of_run_id, created_at, run_id);
