CREATE TABLE IF NOT EXISTS metadata_last_sync_state (
    job_name TEXT PRIMARY KEY NOT NULL,
    last_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sync_batch_id BIGINT NOT NULL,
    normalize_batch_id BIGINT
);

CREATE TABLE IF NOT EXISTS metadata_qrep_partitions (
    job_name TEXT NOT NULL,
    partition_id TEXT NOT NULL,
    sync_partition JSON NOT NULL,
    sync_start_time TIMESTAMPTZ NOT NULL,
    sync_finish_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_name, partition_id)
);
