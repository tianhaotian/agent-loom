ALTER TABLE agent_executions
    ADD COLUMN status_poll_at timestamp(6) NULL;

UPDATE agent_executions
SET status_poll_at = UTC_TIMESTAMP(6)
WHERE status = 'reconciling' AND remote_run_ref IS NOT NULL AND status_poll_at IS NULL;

CREATE INDEX ix_agent_executions_status_poll
    ON agent_executions (tenant_id, status, status_poll_at, agent_execution_id);
