ALTER TABLE metadata_last_sync_state ADD COLUMN IF NOT EXISTS last_text text NOT NULL DEFAULT '';
ALTER TABLE peerdb_stats.cdc_batches ADD COLUMN IF NOT EXISTS batch_end_lsn_text text NOT NULL DEFAULT '';

