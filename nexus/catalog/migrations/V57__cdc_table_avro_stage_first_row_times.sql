ALTER TABLE cdc_table_avro_stage ADD COLUMN IF NOT EXISTS first_row_received_at TIMESTAMP;
COMMENT ON COLUMN cdc_table_avro_stage.first_row_received_at IS
    'Wall-clock time (UTC) when PeerDB received the first row event of this table batch; used for per-table destination lag';
ALTER TABLE cdc_table_avro_stage ADD COLUMN IF NOT EXISTS first_row_commit_time TIMESTAMP;
COMMENT ON COLUMN cdc_table_avro_stage.first_row_commit_time IS
    'Commit time (UTC) of the first row event of this table batch; used for per-table end-to-end lag';
