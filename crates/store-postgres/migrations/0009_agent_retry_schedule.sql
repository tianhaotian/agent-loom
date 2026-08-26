ALTER TABLE agent_loom.agent_executions
    ADD COLUMN retry_at timestamptz(6);

ALTER TABLE agent_loom.agent_executions
    ADD CONSTRAINT ck_agent_execs__retry_schedule CHECK (
        retry_at IS NULL OR status = 'reconciling'
    );

CREATE INDEX ix_agent_execs__retry_due
    ON agent_loom.agent_executions (status, retry_at, agent_execution_id);
