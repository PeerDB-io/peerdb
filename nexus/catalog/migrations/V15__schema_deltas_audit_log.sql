CREATE TABLE IF NOT EXISTS peerdb_stats.schema_deltas_audit_log (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    flow_job_name TEXT NOT NULL,
    read_timestamp TIMESTAMP DEFAULT now(),
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    delta_info JSONB NOT NULL
);