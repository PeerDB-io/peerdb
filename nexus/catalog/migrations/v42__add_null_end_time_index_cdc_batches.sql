CREATE INDEX IF NOT EXISTS idx_cdc_batches_end_time_null ON peerdb_stats.cdc_batches(end_time) WHERE end_time IS NULL;
