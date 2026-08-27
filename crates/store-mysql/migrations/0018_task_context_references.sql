CREATE TABLE task_context_references (
    tenant_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    task_id binary(16) NOT NULL,
    context_snapshot_id binary(16) NOT NULL,
    projection_json json NOT NULL,
    created_event_id binary(16) NOT NULL,
    created_at datetime(6) NOT NULL,
    CONSTRAINT pk_task_context_references PRIMARY KEY (task_id),
    CONSTRAINT uq_task_context_references__tenant UNIQUE (tenant_id, task_id),
    CONSTRAINT fk_task_context_references__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES tasks (tenant_id, run_id, task_id),
    CONSTRAINT fk_task_context_references__snapshot
        FOREIGN KEY (tenant_id, run_id, context_snapshot_id)
        REFERENCES context_snapshots (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT fk_task_context_references__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES events (tenant_id, run_id, event_id),
    CONSTRAINT ck_task_context_references__projection CHECK (json_type(projection_json) = 'ARRAY') ENFORCED
) ENGINE=InnoDB;

CREATE INDEX ix_task_context_references__snapshot
    ON task_context_references (tenant_id, run_id, context_snapshot_id, task_id);

INSERT INTO task_context_references (
    tenant_id, run_id, task_id, context_snapshot_id, projection_json, created_event_id, created_at
)
SELECT t.tenant_id, t.run_id, t.task_id, r.current_context_snapshot_id,
       JSON_ARRAY(), t.created_event_id, t.created_at
FROM tasks t
JOIN runs r ON r.tenant_id = t.tenant_id AND r.run_id = t.run_id;
