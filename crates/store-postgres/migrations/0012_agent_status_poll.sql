ALTER TABLE agent_loom.agent_executions
    ADD COLUMN IF NOT EXISTS status_poll_at timestamptz;

UPDATE agent_loom.agent_executions
SET status_poll_at = clock_timestamp()
WHERE status = 'reconciling' AND remote_run_ref IS NOT NULL AND status_poll_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_agent_executions_status_poll
    ON agent_loom.agent_executions (tenant_id, status_poll_at, agent_execution_id)
    WHERE status = 'reconciling' AND remote_run_ref IS NOT NULL;
