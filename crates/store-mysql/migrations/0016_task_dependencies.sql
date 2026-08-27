ALTER TABLE tasks
    ADD COLUMN join_policy varchar(16) COLLATE utf8mb4_0900_bin NOT NULL DEFAULT 'all',
    ADD CONSTRAINT ck_tasks__join_policy CHECK (join_policy IN ('all', 'any')) ENFORCED;

CREATE TABLE task_dependencies (
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    task_id binary(16) NOT NULL,
    prerequisite_task_id binary(16) NOT NULL,
    condition_json json NOT NULL,
    created_event_id binary(16) NOT NULL,
    created_at datetime(6) NOT NULL,
    CONSTRAINT pk_task_dependencies PRIMARY KEY
        (tenant_id, run_id, task_id, prerequisite_task_id),
    CONSTRAINT fk_task_dependencies__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_task_dependencies__prerequisite
        FOREIGN KEY (tenant_id, run_id, prerequisite_task_id)
        REFERENCES tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_task_dependencies__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_task_dependencies__self CHECK (task_id <> prerequisite_task_id) ENFORCED,
    INDEX ix_task_dependencies__prerequisite
        (tenant_id, run_id, prerequisite_task_id, task_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;
