ALTER TABLE peerdb_stats.qrep_partitions ALTER COLUMN rows_in_partition TYPE bigint;

ALTER TABLE peerdb_stats.cdc_batch_table ALTER COLUMN insert_count TYPE bigint;
ALTER TABLE peerdb_stats.cdc_batch_table ALTER COLUMN update_count TYPE bigint;
ALTER TABLE peerdb_stats.cdc_batch_table ALTER COLUMN delete_count TYPE bigint;

ALTER TABLE peerdb_stats.cdc_batches ALTER COLUMN rows_in_batch TYPE bigint;

