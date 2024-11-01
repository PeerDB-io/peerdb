CREATE SCHEMA IF NOT EXISTS maintenance;

CREATE TABLE IF NOT EXISTS maintenance.maintenance_flows
(
    id              SERIAL PRIMARY KEY,
    flow_id         serial    NOT NULL,
    flow_name       TEXT      NOT NULL,
    workflow_id     TEXT      NOT NULL,
    flow_created_at TIMESTAMP NOT NULL,
    is_cdc          BOOLEAN   NOT NULL,
    state           TEXT      NOT NULL,
    restored_at     TIMESTAMP DEFAULT NULL,
    from_version   TEXT      DEFAULT NULL,
    to_version     TEXT      DEFAULT NULL
);
