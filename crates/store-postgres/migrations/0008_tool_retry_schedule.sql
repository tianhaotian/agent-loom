ALTER TABLE agent_loom.tool_executions
    ADD COLUMN retry_at timestamptz(6);

UPDATE agent_loom.tool_executions
SET retry_at = updated_at
WHERE status = 'retry_scheduled';

ALTER TABLE agent_loom.tool_executions
    ADD CONSTRAINT ck_tool_execs__retry_schedule CHECK (
        (status = 'retry_scheduled' AND retry_at IS NOT NULL)
        OR (status <> 'retry_scheduled' AND retry_at IS NULL)
    );

CREATE INDEX ix_tool_execs__retry_due
    ON agent_loom.tool_executions (status, retry_at, tool_execution_id);
