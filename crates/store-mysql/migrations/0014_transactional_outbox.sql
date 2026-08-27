CREATE TABLE outbox_messages (
    outbox_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    event_id binary(16) NOT NULL,
    run_id binary(16) NOT NULL,
    topic varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    partition_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    payload_json json NOT NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    attempt bigint NOT NULL,
    available_at datetime(6) NOT NULL,
    lease_owner binary(16) NULL,
    lease_token binary(32) NULL,
    lease_expires_at datetime(6) NULL,
    last_error_code varchar(255) COLLATE utf8mb4_0900_bin NULL,
    created_at datetime(6) NOT NULL,
    published_at datetime(6) NULL,
    CONSTRAINT pk_outbox_messages PRIMARY KEY (outbox_id),
    CONSTRAINT uq_outbox_messages__tenant_id UNIQUE (tenant_id, outbox_id),
    CONSTRAINT uq_outbox_messages__event_topic UNIQUE (tenant_id, event_id, topic),
    CONSTRAINT fk_outbox_messages__event FOREIGN KEY (tenant_id, run_id, event_id)
        REFERENCES events (tenant_id, run_id, event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_outbox_messages__status CHECK (status IN ('pending', 'publishing', 'published')) ENFORCED,
    CONSTRAINT ck_outbox_messages__attempt CHECK (attempt >= 0) ENFORCED,
    CONSTRAINT ck_outbox_messages__identity CHECK (
        char_length(topic) > 0 AND char_length(partition_key) > 0) ENFORCED,
    CONSTRAINT ck_outbox_messages__lease CHECK (
        (status = 'publishing' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL)
        OR (status <> 'publishing' AND lease_owner IS NULL AND lease_token IS NULL
            AND lease_expires_at IS NULL)
    ) ENFORCED,
    CONSTRAINT ck_outbox_messages__published CHECK (
        (status = 'published' AND published_at IS NOT NULL)
        OR (status <> 'published' AND published_at IS NULL)
    ) ENFORCED,
    INDEX ix_outbox_messages__claim (status, available_at, outbox_id),
    INDEX ix_outbox_messages__lease (status, lease_expires_at, outbox_id),
    INDEX ix_outbox_messages__diagnostic (tenant_id, partition_key, created_at, outbox_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;
