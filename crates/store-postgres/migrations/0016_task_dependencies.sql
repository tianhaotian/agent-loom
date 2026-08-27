ALTER TABLE agent_loom.tasks
    ADD COLUMN join_policy varchar(16) COLLATE "C" NOT NULL DEFAULT 'all',
    ADD CONSTRAINT ck_tasks__join_policy CHECK (join_policy IN ('all', 'any'));

CREATE TABLE agent_loom.task_dependencies (
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    task_id uuid NOT NULL,
    prerequisite_task_id uuid NOT NULL,
    condition_json jsonb NOT NULL,
    created_event_id uuid NOT NULL,
    created_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_task_dependencies PRIMARY KEY
        (tenant_id, run_id, task_id, prerequisite_task_id),
    CONSTRAINT fk_task_dependencies__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES agent_loom.tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_task_dependencies__prerequisite
        FOREIGN KEY (tenant_id, run_id, prerequisite_task_id)
        REFERENCES agent_loom.tasks (tenant_id, run_id, task_id) ON DELETE RESTRICT,
    CONSTRAINT fk_task_dependencies__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_task_dependencies__self CHECK (task_id <> prerequisite_task_id)
);

CREATE INDEX ix_task_dependencies__prerequisite
    ON agent_loom.task_dependencies
        (tenant_id, run_id, prerequisite_task_id, task_id);
