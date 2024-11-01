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
    from_version   TEXT,
    to_version     TEXT
);
