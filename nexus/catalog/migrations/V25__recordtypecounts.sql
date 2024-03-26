ALTER TABLE peerdb_stats.cdc_batch_table
ADD COLUMN insert_count INTEGER,
ADD COLUMN update_count INTEGER,
ADD COLUMN delete_count INTEGER;
