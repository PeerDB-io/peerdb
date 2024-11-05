CREATE SCHEMA IF NOT EXISTS maintenance;

CREATE TABLE IF NOT EXISTS maintenance.maintenance_flows
(
    id              serial PRIMARY KEY,
    flow_id         bigint    NOT NULL,
    flow_name       TEXT      NOT NULL,
    workflow_id     TEXT      NOT NULL,
    flow_created_at TIMESTAMP NOT NULL,
    is_cdc          BOOLEAN   NOT NULL,
    state           TEXT      NOT NULL,
    restored_at     TIMESTAMP,
    from_version    TEXT,
    to_version      TEXT
);

CREATE TABLE IF NOT EXISTS maintenance.start_maintenance_outputs
(
    id             serial PRIMARY KEY,
    api_version    TEXT      NOT NULL,
    cli_version    TEXT      NOT NULL,
    skipped        BOOLEAN   NOT NULL,
    skipped_reason TEXT,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_start_maintenance_outputs_created_at ON maintenance.start_maintenance_outputs (created_at DESC);
