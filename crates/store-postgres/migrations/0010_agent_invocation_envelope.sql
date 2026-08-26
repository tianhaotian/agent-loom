ALTER TABLE agent_loom.agent_executions
    ADD COLUMN request_json jsonb;

UPDATE agent_loom.agent_executions
SET request_json = '{}'::jsonb
WHERE request_json IS NULL;

ALTER TABLE agent_loom.agent_executions
    ALTER COLUMN request_json SET NOT NULL;
