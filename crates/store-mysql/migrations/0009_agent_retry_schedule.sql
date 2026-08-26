ALTER TABLE agent_executions
    ADD COLUMN retry_at datetime(6) NULL,
    ADD CONSTRAINT ck_agent_execs__retry_schedule CHECK (
        retry_at IS NULL OR status = 'reconciling'
    ) ENFORCED,
    ADD INDEX ix_agent_execs__retry_due (status, retry_at, agent_execution_id);
