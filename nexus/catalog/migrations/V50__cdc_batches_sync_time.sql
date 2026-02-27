ALTER TABLE peerdb_stats.cdc_batches ADD COLUMN IF NOT EXISTS sync_time TIMESTAMP;
