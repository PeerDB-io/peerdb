ALTER TABLE peerdb_stats.cdc_batches ADD COLUMN IF NOT EXISTS first_row_received_at TIMESTAMP;
COMMENT ON COLUMN peerdb_stats.cdc_batches.first_row_received_at IS
    'Wall-clock time (UTC) when PeerDB received the first row event of the batch; used for destination lag';
ALTER TABLE peerdb_stats.cdc_batches ADD COLUMN IF NOT EXISTS first_row_commit_time TIMESTAMP;
COMMENT ON COLUMN peerdb_stats.cdc_batches.first_row_commit_time IS
    'Commit time (UTC) of the first row event of the batch, shifted into the worker clock frame '
    '(source commit minus source/worker clock offset); used for skew-corrected end-to-end lag';
