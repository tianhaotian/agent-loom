CREATE TABLE agent_loom.agent_endpoints (
    endpoint_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    endpoint_key varchar(255) COLLATE "C" NOT NULL,
    adapter_kind varchar(128) COLLATE "C" NOT NULL,
    base_uri text NOT NULL,
    protocol_version varchar(128) COLLATE "C" NOT NULL,
    capabilities_json jsonb NOT NULL,
    credential_ref varchar(512) COLLATE "C" NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    health_checked_at timestamptz(6),
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_agent_endpoints PRIMARY KEY (endpoint_id),
    CONSTRAINT uq_endpoints__tenant_id UNIQUE (tenant_id, endpoint_id),
    CONSTRAINT uq_endpoints__tenant_key UNIQUE (tenant_id, endpoint_key),
    CONSTRAINT fk_endpoints__tenant FOREIGN KEY (tenant_id)
        REFERENCES agent_loom.tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT ck_endpoints__key_nonempty CHECK (length(endpoint_key) > 0),
    CONSTRAINT ck_endpoints__adapter_nonempty CHECK (length(adapter_kind) > 0),
    CONSTRAINT ck_endpoints__base_uri_nonempty CHECK (length(base_uri) > 0),
    CONSTRAINT ck_endpoints__protocol_nonempty CHECK (length(protocol_version) > 0),
    CONSTRAINT ck_endpoints__credential_nonempty CHECK (length(credential_ref) > 0),
    CONSTRAINT ck_endpoints__status CHECK (status IN ('active', 'disabled')),
    CONSTRAINT ck_endpoints__time CHECK (updated_at >= created_at)
);

CREATE INDEX ix_endpoints__adapter_status
    ON agent_loom.agent_endpoints (tenant_id, adapter_kind, status, endpoint_id);
