CREATE TABLE agent_endpoints (
    endpoint_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    endpoint_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    adapter_kind varchar(128) COLLATE utf8mb4_0900_bin NOT NULL,
    base_uri text NOT NULL,
    protocol_version varchar(128) COLLATE utf8mb4_0900_bin NOT NULL,
    capabilities_json json NOT NULL,
    credential_ref varchar(512) COLLATE utf8mb4_0900_bin NOT NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    health_checked_at datetime(6) NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    CONSTRAINT pk_agent_endpoints PRIMARY KEY (endpoint_id),
    CONSTRAINT uq_endpoints__tenant_id UNIQUE (tenant_id, endpoint_id),
    CONSTRAINT uq_endpoints__tenant_key UNIQUE (tenant_id, endpoint_key),
    CONSTRAINT fk_endpoints__tenant FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT ck_endpoints__key_nonempty
        CHECK (char_length(endpoint_key) > 0) ENFORCED,
    CONSTRAINT ck_endpoints__adapter_nonempty
        CHECK (char_length(adapter_kind) > 0) ENFORCED,
    CONSTRAINT ck_endpoints__base_uri_nonempty
        CHECK (char_length(base_uri) > 0) ENFORCED,
    CONSTRAINT ck_endpoints__protocol_nonempty
        CHECK (char_length(protocol_version) > 0) ENFORCED,
    CONSTRAINT ck_endpoints__credential_nonempty
        CHECK (char_length(credential_ref) > 0) ENFORCED,
    CONSTRAINT ck_endpoints__status
        CHECK (status IN ('active', 'disabled')) ENFORCED,
    CONSTRAINT ck_endpoints__time CHECK (updated_at >= created_at) ENFORCED,
    INDEX ix_endpoints__adapter_status
        (tenant_id, adapter_kind, status, endpoint_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;
