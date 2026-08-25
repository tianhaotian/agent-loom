CREATE TABLE agent_loom.tenants (
    tenant_id uuid NOT NULL,
    tenant_key varchar(255) COLLATE "C" NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    policy_json jsonb NOT NULL,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_tenants PRIMARY KEY (tenant_id),
    CONSTRAINT uq_tenants__key UNIQUE (tenant_key),
    CONSTRAINT ck_tenants__key_nonempty CHECK (length(tenant_key) > 0),
    CONSTRAINT ck_tenants__status
        CHECK (status IN ('active', 'suspended', 'deleting')),
    CONSTRAINT ck_tenants__time CHECK (updated_at >= created_at)
);

CREATE INDEX ix_tenants__status
    ON agent_loom.tenants (status, updated_at, tenant_id);

CREATE TABLE agent_loom.workflow_definitions (
    workflow_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    workflow_key varchar(255) COLLATE "C" NOT NULL,
    name varchar(512) NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    latest_version bigint,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_workflow_definitions PRIMARY KEY (workflow_id),
    CONSTRAINT uq_workflows__tenant_id UNIQUE (tenant_id, workflow_id),
    CONSTRAINT uq_workflows__tenant_key UNIQUE (tenant_id, workflow_key),
    CONSTRAINT fk_workflows__tenant FOREIGN KEY (tenant_id)
        REFERENCES agent_loom.tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT ck_workflows__key_nonempty CHECK (length(workflow_key) > 0),
    CONSTRAINT ck_workflows__name_nonempty CHECK (length(name) > 0),
    CONSTRAINT ck_workflows__status CHECK (status IN ('active', 'archived')),
    CONSTRAINT ck_workflows__latest_version
        CHECK (latest_version IS NULL OR latest_version >= 1),
    CONSTRAINT ck_workflows__time CHECK (updated_at >= created_at)
);

CREATE TABLE agent_loom.workflow_definition_versions (
    workflow_version_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    version bigint NOT NULL,
    lifecycle varchar(32) COLLATE "C" NOT NULL,
    spec_json jsonb NOT NULL,
    spec_digest bytea NOT NULL,
    created_by varchar(512) COLLATE "C" NOT NULL,
    created_at timestamptz(6) NOT NULL,
    published_at timestamptz(6),
    CONSTRAINT pk_workflow_versions PRIMARY KEY (workflow_version_id),
    CONSTRAINT uq_workflow_versions__tenant_id
        UNIQUE (tenant_id, workflow_version_id),
    CONSTRAINT uq_workflow_versions__number
        UNIQUE (tenant_id, workflow_id, version),
    CONSTRAINT fk_workflow_versions__workflow
        FOREIGN KEY (tenant_id, workflow_id)
        REFERENCES agent_loom.workflow_definitions (tenant_id, workflow_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_workflow_versions__version CHECK (version >= 1),
    CONSTRAINT ck_workflow_versions__lifecycle
        CHECK (lifecycle IN ('draft', 'published', 'retired')),
    CONSTRAINT ck_workflow_versions__digest
        CHECK (octet_length(spec_digest) = 32),
    CONSTRAINT ck_workflow_versions__creator CHECK (length(created_by) > 0),
    CONSTRAINT ck_workflow_versions__published_at
        CHECK ((lifecycle = 'draft' AND published_at IS NULL)
            OR (lifecycle IN ('published', 'retired') AND published_at IS NOT NULL))
);

CREATE TABLE agent_loom.agent_definitions (
    agent_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    agent_key varchar(255) COLLATE "C" NOT NULL,
    name varchar(512) NOT NULL,
    status varchar(32) COLLATE "C" NOT NULL,
    latest_version bigint,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    CONSTRAINT pk_agent_definitions PRIMARY KEY (agent_id),
    CONSTRAINT uq_agents__tenant_id UNIQUE (tenant_id, agent_id),
    CONSTRAINT uq_agents__tenant_key UNIQUE (tenant_id, agent_key),
    CONSTRAINT fk_agents__tenant FOREIGN KEY (tenant_id)
        REFERENCES agent_loom.tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT ck_agents__key_nonempty CHECK (length(agent_key) > 0),
    CONSTRAINT ck_agents__name_nonempty CHECK (length(name) > 0),
    CONSTRAINT ck_agents__status CHECK (status IN ('active', 'archived')),
    CONSTRAINT ck_agents__latest_version
        CHECK (latest_version IS NULL OR latest_version >= 1),
    CONSTRAINT ck_agents__time CHECK (updated_at >= created_at)
);

CREATE TABLE agent_loom.agent_definition_versions (
    agent_version_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    version bigint NOT NULL,
    lifecycle varchar(32) COLLATE "C" NOT NULL,
    system_instructions text NOT NULL,
    model_config_json jsonb NOT NULL,
    tools_json jsonb NOT NULL,
    capabilities_json jsonb NOT NULL,
    handoff_json jsonb NOT NULL,
    guardrails_json jsonb NOT NULL,
    limits_json jsonb NOT NULL,
    spec_digest bytea NOT NULL,
    created_by varchar(512) COLLATE "C" NOT NULL,
    created_at timestamptz(6) NOT NULL,
    published_at timestamptz(6),
    CONSTRAINT pk_agent_versions PRIMARY KEY (agent_version_id),
    CONSTRAINT uq_agent_versions__tenant_id UNIQUE (tenant_id, agent_version_id),
    CONSTRAINT uq_agent_versions__number UNIQUE (tenant_id, agent_id, version),
    CONSTRAINT fk_agent_versions__agent FOREIGN KEY (tenant_id, agent_id)
        REFERENCES agent_loom.agent_definitions (tenant_id, agent_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_agent_versions__version CHECK (version >= 1),
    CONSTRAINT ck_agent_versions__lifecycle
        CHECK (lifecycle IN ('draft', 'published', 'retired')),
    CONSTRAINT ck_agent_versions__instructions
        CHECK (length(system_instructions) > 0),
    CONSTRAINT ck_agent_versions__digest CHECK (octet_length(spec_digest) = 32),
    CONSTRAINT ck_agent_versions__creator CHECK (length(created_by) > 0),
    CONSTRAINT ck_agent_versions__published_at
        CHECK ((lifecycle = 'draft' AND published_at IS NULL)
            OR (lifecycle IN ('published', 'retired') AND published_at IS NOT NULL))
);
