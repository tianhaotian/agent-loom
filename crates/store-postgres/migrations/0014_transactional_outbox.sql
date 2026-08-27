CREATE TABLE agent_loom.outbox_messages (
    outbox_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    event_id uuid NOT NULL,
    run_id uuid NOT NULL,
    topic varchar(255) COLLATE "C" NOT NULL,
    partition_key varchar(255) COLLATE "C" NOT NULL,
    payload_json jsonb NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    attempt bigint NOT NULL,
    available_at timestamptz(6) NOT NULL,
    lease_owner uuid,
    lease_token bytea,
    lease_expires_at timestamptz(6),
    last_error_code varchar(255) COLLATE "C",
    created_at timestamptz(6) NOT NULL,
    published_at timestamptz(6),
    CONSTRAINT pk_outbox_messages PRIMARY KEY (outbox_id),
    CONSTRAINT uq_outbox_messages__tenant_id UNIQUE (tenant_id, outbox_id),
    CONSTRAINT uq_outbox_messages__event_topic UNIQUE (tenant_id, event_id, topic),
    CONSTRAINT fk_outbox_messages__event FOREIGN KEY (tenant_id, run_id, event_id)
        REFERENCES agent_loom.events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_outbox_messages__status CHECK (status IN ('pending', 'publishing', 'published')),
    CONSTRAINT ck_outbox_messages__attempt CHECK (attempt >= 0),
    CONSTRAINT ck_outbox_messages__identity CHECK (length(topic) > 0 AND length(partition_key) > 0),
    CONSTRAINT ck_outbox_messages__lease CHECK (
        (status = 'publishing' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL
            AND octet_length(lease_token) = 32 AND lease_expires_at IS NOT NULL)
        OR (status <> 'publishing' AND lease_owner IS NULL AND lease_token IS NULL
            AND lease_expires_at IS NULL)
    ),
    CONSTRAINT ck_outbox_messages__published CHECK (
        (status = 'published' AND published_at IS NOT NULL)
        OR (status <> 'published' AND published_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_outbox_messages__claim
    ON agent_loom.outbox_messages (status, available_at, outbox_id);
CREATE INDEX IF NOT EXISTS ix_outbox_messages__lease
    ON agent_loom.outbox_messages (status, lease_expires_at, outbox_id);
CREATE INDEX IF NOT EXISTS ix_outbox_messages__diagnostic
    ON agent_loom.outbox_messages (tenant_id, partition_key, created_at, outbox_id);
