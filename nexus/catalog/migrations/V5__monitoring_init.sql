CREATE SCHEMA IF NOT EXISTS peerdb_stats;

CREATE TABLE IF NOT EXISTS peerdb_stats.cdc_flows (
    flow_name TEXT PRIMARY KEY,
    latest_lsn_at_source NUMERIC NOT NULL,
    latest_lsn_at_target NUMERIC NOT NULL,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS peerdb_stats.cdc_batches (
    flow_name TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    rows_in_batch INTEGER NOT NULL,
    batch_start_lsn NUMERIC NOT NULL,
    batch_end_lsn NUMERIC NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS peerdb_stats.cdc_batch_table (
    flow_name TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    destination_table_name TEXT NOT NULL,
    num_rows BIGINT NOT NULL,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS peerdb_stats.qrep_runs (
    flow_name TEXT NOT NULL,
    run_uuid TEXT NOT NULL,
    num_rows_to_sync BIGINT,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS peerdb_stats.qrep_partitions (
    flow_name TEXT NOT NULL,
    run_uuid TEXT NOT NULL,
    partition_uuid TEXT NOT NULL,
    partition_start TEXT NOT NULL,
    partition_end TEXT NOT NULL,
    rows_in_partition INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    pull_end_time TIMESTAMP,
    end_time TIMESTAMP,
    restart_count INTEGER NOT NULL,
    metadata JSONB
);


