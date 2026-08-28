ALTER TABLE agent_loom.runs
    ADD COLUMN replay_of_run_id uuid,
    ADD CONSTRAINT fk_runs__replay_source
        FOREIGN KEY (tenant_id, replay_of_run_id)
        REFERENCES agent_loom.runs (tenant_id, run_id) ON DELETE RESTRICT,
    ADD CONSTRAINT ck_runs__replay_not_self
        CHECK (replay_of_run_id IS NULL OR replay_of_run_id <> run_id);

CREATE INDEX ix_runs__replay_source
    ON agent_loom.runs (tenant_id, replay_of_run_id, created_at, run_id)
    WHERE replay_of_run_id IS NOT NULL;
