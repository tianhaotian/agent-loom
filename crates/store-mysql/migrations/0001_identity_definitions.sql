CREATE TABLE tenants (
    tenant_id binary(16) NOT NULL,
    tenant_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    policy_json json NOT NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    CONSTRAINT pk_tenants PRIMARY KEY (tenant_id),
    CONSTRAINT uq_tenants__key UNIQUE (tenant_key),
    CONSTRAINT ck_tenants__key_nonempty CHECK (char_length(tenant_key) > 0) ENFORCED,
    CONSTRAINT ck_tenants__status
        CHECK (status IN ('active', 'suspended', 'deleting')) ENFORCED,
    CONSTRAINT ck_tenants__time CHECK (updated_at >= created_at) ENFORCED,
    INDEX ix_tenants__status (status, updated_at, tenant_id)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE workflow_definitions (
    workflow_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    workflow_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    name varchar(512) NOT NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    latest_version bigint NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    CONSTRAINT pk_workflow_definitions PRIMARY KEY (workflow_id),
    CONSTRAINT uq_workflows__tenant_id UNIQUE (tenant_id, workflow_id),
    CONSTRAINT uq_workflows__tenant_key UNIQUE (tenant_id, workflow_key),
    CONSTRAINT fk_workflows__tenant FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT ck_workflows__key_nonempty
        CHECK (char_length(workflow_key) > 0) ENFORCED,
    CONSTRAINT ck_workflows__name_nonempty CHECK (char_length(name) > 0) ENFORCED,
    CONSTRAINT ck_workflows__status
        CHECK (status IN ('active', 'archived')) ENFORCED,
    CONSTRAINT ck_workflows__latest_version
        CHECK (latest_version IS NULL OR latest_version >= 1) ENFORCED,
    CONSTRAINT ck_workflows__time CHECK (updated_at >= created_at) ENFORCED
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE workflow_definition_versions (
    workflow_version_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    workflow_id binary(16) NOT NULL,
    version bigint NOT NULL,
    lifecycle varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    spec_json json NOT NULL,
    spec_digest binary(32) NOT NULL,
    created_by varchar(512) COLLATE utf8mb4_0900_bin NOT NULL,
    created_at datetime(6) NOT NULL,
    published_at datetime(6) NULL,
    CONSTRAINT pk_workflow_versions PRIMARY KEY (workflow_version_id),
    CONSTRAINT uq_workflow_versions__tenant_id
        UNIQUE (tenant_id, workflow_version_id),
    CONSTRAINT uq_workflow_versions__number
        UNIQUE (tenant_id, workflow_id, version),
    CONSTRAINT fk_workflow_versions__workflow
        FOREIGN KEY (tenant_id, workflow_id)
        REFERENCES workflow_definitions (tenant_id, workflow_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_workflow_versions__version CHECK (version >= 1) ENFORCED,
    CONSTRAINT ck_workflow_versions__lifecycle
        CHECK (lifecycle IN ('draft', 'published', 'retired')) ENFORCED,
    CONSTRAINT ck_workflow_versions__creator
        CHECK (char_length(created_by) > 0) ENFORCED,
    CONSTRAINT ck_workflow_versions__published_at
        CHECK ((lifecycle = 'draft' AND published_at IS NULL)
            OR (lifecycle IN ('published', 'retired') AND published_at IS NOT NULL)) ENFORCED
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE agent_definitions (
    agent_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    agent_key varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
    name varchar(512) NOT NULL,
    status varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    latest_version bigint NULL,
    created_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL,
    CONSTRAINT pk_agent_definitions PRIMARY KEY (agent_id),
    CONSTRAINT uq_agents__tenant_id UNIQUE (tenant_id, agent_id),
    CONSTRAINT uq_agents__tenant_key UNIQUE (tenant_id, agent_key),
    CONSTRAINT fk_agents__tenant FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT ck_agents__key_nonempty CHECK (char_length(agent_key) > 0) ENFORCED,
    CONSTRAINT ck_agents__name_nonempty CHECK (char_length(name) > 0) ENFORCED,
    CONSTRAINT ck_agents__status
        CHECK (status IN ('active', 'archived')) ENFORCED,
    CONSTRAINT ck_agents__latest_version
        CHECK (latest_version IS NULL OR latest_version >= 1) ENFORCED,
    CONSTRAINT ck_agents__time CHECK (updated_at >= created_at) ENFORCED
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

CREATE TABLE agent_definition_versions (
    agent_version_id binary(16) NOT NULL,
    tenant_id binary(16) NOT NULL,
    agent_id binary(16) NOT NULL,
    version bigint NOT NULL,
    lifecycle varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    system_instructions text NOT NULL,
    model_config_json json NOT NULL,
    tools_json json NOT NULL,
    capabilities_json json NOT NULL,
    handoff_json json NOT NULL,
    guardrails_json json NOT NULL,
    limits_json json NOT NULL,
    spec_digest binary(32) NOT NULL,
    created_by varchar(512) COLLATE utf8mb4_0900_bin NOT NULL,
    created_at datetime(6) NOT NULL,
    published_at datetime(6) NULL,
    CONSTRAINT pk_agent_versions PRIMARY KEY (agent_version_id),
    CONSTRAINT uq_agent_versions__tenant_id UNIQUE (tenant_id, agent_version_id),
    CONSTRAINT uq_agent_versions__number UNIQUE (tenant_id, agent_id, version),
    CONSTRAINT fk_agent_versions__agent FOREIGN KEY (tenant_id, agent_id)
        REFERENCES agent_definitions (tenant_id, agent_id) ON DELETE RESTRICT,
    CONSTRAINT ck_agent_versions__version CHECK (version >= 1) ENFORCED,
    CONSTRAINT ck_agent_versions__lifecycle
        CHECK (lifecycle IN ('draft', 'published', 'retired')) ENFORCED,
    CONSTRAINT ck_agent_versions__instructions
        CHECK (char_length(system_instructions) > 0) ENFORCED,
    CONSTRAINT ck_agent_versions__creator
        CHECK (char_length(created_by) > 0) ENFORCED,
    CONSTRAINT ck_agent_versions__published_at
        CHECK ((lifecycle = 'draft' AND published_at IS NULL)
            OR (lifecycle IN ('published', 'retired') AND published_at IS NOT NULL)) ENFORCED
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;
