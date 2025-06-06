ALTER TABLE metadata_last_sync_state
  ADD COLUMN IF NOT EXISTS latest_batch_id_in_raw_table BIGINT,
  ADD COLUMN IF NOT EXISTS table_batch_id_data JSONB default '{}';
