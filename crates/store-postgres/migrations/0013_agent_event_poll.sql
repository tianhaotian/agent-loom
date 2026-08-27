UPDATE agent_loom.agent_executions
SET status_poll_at = clock_timestamp()
WHERE status = 'running'
  AND remote_run_ref IS NOT NULL
  AND remote_protocol_version IS NOT NULL
  AND status_poll_at IS NULL;

CREATE INDEX IF NOT EXISTS agent_executions_event_poll_idx
    ON agent_loom.agent_executions (tenant_id, status_poll_at, agent_execution_id)
    WHERE status = 'running' AND remote_run_ref IS NOT NULL;
