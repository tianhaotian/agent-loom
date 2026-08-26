ALTER TABLE agent_loom.wait_subscriptions
    ADD COLUMN resume_task_id uuid,
    ADD COLUMN resume_logical_key varchar(255) COLLATE "C",
    ADD COLUMN resume_task_kind varchar(64) COLLATE "C",
    ADD COLUMN resume_priority integer,
    ADD COLUMN resume_max_attempts bigint,
    ADD COLUMN resume_input_json jsonb,
    ADD COLUMN resume_deadline timestamptz(6);

UPDATE agent_loom.wait_subscriptions
SET resume_task_id = md5(wait_id::text || ':resume')::uuid,
    resume_logical_key = 'legacy/wait/' || wait_id::text || '/resume',
    resume_task_kind = 'reconcile',
    resume_priority = 0,
    resume_max_attempts = 3,
    resume_input_json = '{}'::jsonb;

ALTER TABLE agent_loom.wait_subscriptions
    ALTER COLUMN resume_task_id SET NOT NULL,
    ALTER COLUMN resume_logical_key SET NOT NULL,
    ALTER COLUMN resume_task_kind SET NOT NULL,
    ALTER COLUMN resume_priority SET NOT NULL,
    ALTER COLUMN resume_max_attempts SET NOT NULL,
    ALTER COLUMN resume_input_json SET NOT NULL,
    ADD CONSTRAINT uq_waits__resume_task
        UNIQUE (tenant_id, resume_task_id),
    ADD CONSTRAINT ck_waits__resume_logical_key
        CHECK (length(resume_logical_key) > 0),
    ADD CONSTRAINT ck_waits__resume_task_kind CHECK (resume_task_kind IN (
        'model', 'tool', 'agent_server', 'artifact_check', 'timer_wakeup',
        'reconcile', 'stop_external_execution'
    )),
    ADD CONSTRAINT ck_waits__resume_attempts
        CHECK (resume_max_attempts >= 1),
    ADD CONSTRAINT ck_waits__resume_deadline
        CHECK (resume_deadline IS NULL OR resume_deadline >= created_at);
