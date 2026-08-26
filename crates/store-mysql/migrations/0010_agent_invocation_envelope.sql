ALTER TABLE agent_executions
    ADD COLUMN request_json json NULL;

UPDATE agent_executions
SET request_json = JSON_OBJECT()
WHERE request_json IS NULL;

ALTER TABLE agent_executions
    MODIFY COLUMN request_json json NOT NULL;
