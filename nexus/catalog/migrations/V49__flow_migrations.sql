CREATE TABLE IF NOT EXISTS flow_migrations (
    flow_name text NOT NULL,
    migration_name text NOT NULL,
    completed boolean NOT NULL,
    PRIMARY KEY (flow_name, migration_name)
);
