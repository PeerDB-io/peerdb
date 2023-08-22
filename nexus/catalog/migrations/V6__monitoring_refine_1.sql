ALTER TABLE peerdb_stats.qrep_runs DROP COLUMN num_rows_to_sync;

ALTER TABLE peerdb_stats.qrep_partitions ALTER COLUMN rows_in_partition DROP NOT NULL;

ALTER TABLE peerdb_stats.qrep_partitions ADD UNIQUE(run_uuid,partition_uuid);