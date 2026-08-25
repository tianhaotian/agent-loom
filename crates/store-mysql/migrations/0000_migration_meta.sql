CREATE TABLE schema_migrations (
    logical_id varchar(128) COLLATE utf8mb4_0900_bin NOT NULL,
    provider_kind varchar(32) COLLATE utf8mb4_0900_bin NOT NULL,
    physical_checksum binary(32) NOT NULL,
    logical_model_version bigint NOT NULL,
    state varchar(16) COLLATE utf8mb4_0900_bin NOT NULL,
    started_at datetime(6) NOT NULL,
    applied_at datetime(6) NULL,
    runner_version varchar(64) COLLATE utf8mb4_0900_bin NOT NULL,
    details_json json NOT NULL,
    CONSTRAINT pk_schema_migrations PRIMARY KEY (logical_id),
    CONSTRAINT ck_schema_migrations__model_version
        CHECK (logical_model_version >= 1) ENFORCED,
    CONSTRAINT ck_schema_migrations__state
        CHECK (state IN ('applying', 'applied', 'failed')) ENFORCED,
    CONSTRAINT ck_schema_migrations__applied_at
        CHECK ((state = 'applied') = (applied_at IS NOT NULL)) ENFORCED
) ENGINE=InnoDB
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_bin;

