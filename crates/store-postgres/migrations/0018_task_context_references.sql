CREATE TABLE agent_loom.task_context_references (
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    task_id uuid NOT NULL,
    context_snapshot_id uuid NOT NULL,
    projection_json jsonb NOT NULL,
    created_event_id uuid NOT NULL,
    created_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_task_context_references PRIMARY KEY (task_id),
    CONSTRAINT uq_task_context_references__tenant UNIQUE (tenant_id, task_id),
    CONSTRAINT fk_task_context_references__task FOREIGN KEY (tenant_id, run_id, task_id)
        REFERENCES agent_loom.tasks (tenant_id, run_id, task_id),
    CONSTRAINT fk_task_context_references__snapshot
        FOREIGN KEY (tenant_id, run_id, context_snapshot_id)
        REFERENCES agent_loom.context_snapshots (tenant_id, run_id, context_snapshot_id),
    CONSTRAINT fk_task_context_references__event
        FOREIGN KEY (tenant_id, run_id, created_event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id),
    CONSTRAINT ck_task_context_references__projection CHECK (jsonb_typeof(projection_json) = 'array')
);

CREATE INDEX ix_task_context_references__snapshot
    ON agent_loom.task_context_references (tenant_id, run_id, context_snapshot_id, task_id);

INSERT INTO agent_loom.task_context_references (
    tenant_id, run_id, task_id, context_snapshot_id, projection_json, created_event_id, created_at
)
SELECT t.tenant_id, t.run_id, t.task_id, r.current_context_snapshot_id,
       '[]'::jsonb, t.created_event_id, t.created_at
FROM agent_loom.tasks t
JOIN agent_loom.runs r ON r.tenant_id = t.tenant_id AND r.run_id = t.run_id;
