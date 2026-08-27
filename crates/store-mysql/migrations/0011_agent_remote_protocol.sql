ALTER TABLE agent_executions
    ADD COLUMN remote_protocol_version varchar(128) NULL;

UPDATE agent_executions
SET remote_protocol_version = 'legacy'
WHERE remote_run_ref IS NOT NULL AND remote_protocol_version IS NULL;

ALTER TABLE agent_executions
    ADD CONSTRAINT chk_agent_remote_protocol_nonempty
    CHECK (remote_protocol_version IS NULL OR CHAR_LENGTH(remote_protocol_version) > 0) ENFORCED;
