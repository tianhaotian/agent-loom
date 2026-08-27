UPDATE agent_executions
SET status_poll_at = UTC_TIMESTAMP(6)
WHERE status = 'running'
  AND remote_run_ref IS NOT NULL
  AND remote_protocol_version IS NOT NULL
  AND status_poll_at IS NULL;
