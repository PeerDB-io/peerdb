ALTER TABLE peerdb_stats.cdc_batches ADD COLUMN IF NOT EXISTS sync_time TIMESTAMP;
COMMENT ON COLUMN peerdb_stats.cdc_batches.sync_time IS 'Set via PG NOW() for clock-skew-safe lag calculation';
