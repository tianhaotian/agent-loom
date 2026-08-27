ALTER TABLE agent_loom.tasks
    DROP CONSTRAINT ck_tasks__status;

ALTER TABLE agent_loom.tasks
    ADD CONSTRAINT ck_tasks__status_v2 CHECK (status IN (
        'scheduled', 'queued', 'leased', 'retry_scheduled',
        'succeeded', 'failed', 'dead_lettered', 'skipped', 'cancelled'
    ));

ALTER TABLE agent_loom.tasks
    DROP CONSTRAINT ck_tasks__completion;

ALTER TABLE agent_loom.tasks
    ADD CONSTRAINT ck_tasks__completion_v2 CHECK (
        (status IN ('succeeded', 'failed', 'dead_lettered', 'skipped', 'cancelled')
            AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('succeeded', 'failed', 'dead_lettered', 'skipped', 'cancelled')
            AND completed_at IS NULL)
    );
