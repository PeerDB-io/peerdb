ALTER TABLE peerdb_stats.cdc_batches
    ADD COLUMN IF NOT EXISTS pushed_rows BIGINT DEFAULT 0;
