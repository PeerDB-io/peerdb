CREATE TABLE IF NOT EXISTS metadata_mysql_sync_state (
    job_name TEXT PRIMARY KEY NOT NULL,
    pos_file text,
    pos_offset int,
    gtid text,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sync_batch_id BIGINT NOT NULL,
    normalize_batch_id BIGINT
);

