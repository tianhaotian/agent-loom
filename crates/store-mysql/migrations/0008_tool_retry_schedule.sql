ALTER TABLE tool_executions
    ADD COLUMN retry_at datetime(6) NULL;

UPDATE tool_executions
SET retry_at = updated_at
WHERE status = 'retry_scheduled';

ALTER TABLE tool_executions
    ADD CONSTRAINT ck_tool_execs__retry_schedule CHECK (
        (status = 'retry_scheduled' AND retry_at IS NOT NULL)
        OR (status <> 'retry_scheduled' AND retry_at IS NULL)
    ) ENFORCED,
    ADD INDEX ix_tool_execs__retry_due (status, retry_at, tool_execution_id);
