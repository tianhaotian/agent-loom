ALTER TABLE agent_loom.agent_executions
    ADD COLUMN IF NOT EXISTS remote_protocol_version text;

UPDATE agent_loom.agent_executions
SET remote_protocol_version = 'legacy'
WHERE remote_run_ref IS NOT NULL AND remote_protocol_version IS NULL;

ALTER TABLE agent_loom.agent_executions
    DROP CONSTRAINT IF EXISTS agent_executions_remote_protocol_nonempty;

ALTER TABLE agent_loom.agent_executions
    ADD CONSTRAINT agent_executions_remote_protocol_nonempty
    CHECK (remote_protocol_version IS NULL OR remote_protocol_version <> '');
