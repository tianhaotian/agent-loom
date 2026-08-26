ALTER TABLE wait_subscriptions
    ADD COLUMN resume_task_id binary(16) NULL,
    ADD COLUMN resume_logical_key varchar(255) COLLATE utf8mb4_0900_bin NULL,
    ADD COLUMN resume_task_kind varchar(64) COLLATE utf8mb4_0900_bin NULL,
    ADD COLUMN resume_priority integer NULL,
    ADD COLUMN resume_max_attempts bigint NULL,
    ADD COLUMN resume_input_json json NULL,
    ADD COLUMN resume_deadline datetime(6) NULL;

UPDATE wait_subscriptions
SET resume_task_id = UNHEX(MD5(CONCAT(HEX(wait_id), ':resume'))),
    resume_logical_key = CONCAT('legacy/wait/', LOWER(HEX(wait_id)), '/resume'),
    resume_task_kind = 'reconcile',
    resume_priority = 0,
    resume_max_attempts = 3,
    resume_input_json = JSON_OBJECT();

ALTER TABLE wait_subscriptions
    MODIFY COLUMN resume_task_id binary(16) NOT NULL,
    MODIFY COLUMN resume_logical_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    MODIFY COLUMN resume_task_kind varchar(64) COLLATE utf8mb4_0900_bin NOT NULL,
    MODIFY COLUMN resume_priority integer NOT NULL,
    MODIFY COLUMN resume_max_attempts bigint NOT NULL,
    MODIFY COLUMN resume_input_json json NOT NULL,
    ADD CONSTRAINT uq_waits__resume_task UNIQUE (tenant_id, resume_task_id),
    ADD CONSTRAINT ck_waits__resume_logical_key
        CHECK (char_length(resume_logical_key) > 0) ENFORCED,
    ADD CONSTRAINT ck_waits__resume_task_kind CHECK (resume_task_kind IN (
        'model', 'tool', 'agent_server', 'artifact_check', 'timer_wakeup',
        'reconcile', 'stop_external_execution'
    )) ENFORCED,
    ADD CONSTRAINT ck_waits__resume_attempts
        CHECK (resume_max_attempts >= 1) ENFORCED,
    ADD CONSTRAINT ck_waits__resume_deadline
        CHECK (resume_deadline IS NULL OR resume_deadline >= created_at) ENFORCED;
