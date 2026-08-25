CREATE SCHEMA IF NOT EXISTS agent_loom;

CREATE TABLE agent_loom.schema_migrations (
    logical_id varchar(128) COLLATE "C" PRIMARY KEY,
    provider_kind varchar(32) COLLATE "C" NOT NULL,
    physical_checksum bytea NOT NULL,
    logical_model_version bigint NOT NULL,
    state varchar(16) COLLATE "C" NOT NULL,
    started_at timestamptz(6) NOT NULL,
    applied_at timestamptz(6),
    runner_version varchar(64) COLLATE "C" NOT NULL,
    details_json jsonb NOT NULL,
    CONSTRAINT ck_schema_migrations__checksum
        CHECK (octet_length(physical_checksum) = 32),
    CONSTRAINT ck_schema_migrations__model_version
        CHECK (logical_model_version >= 1),
    CONSTRAINT ck_schema_migrations__state
        CHECK (state IN ('applying', 'applied', 'failed')),
    CONSTRAINT ck_schema_migrations__applied_at
        CHECK ((state = 'applied') = (applied_at IS NOT NULL))
);

