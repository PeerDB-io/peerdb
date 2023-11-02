ALTER TABLE peerdb_stats.qrep_runs
ALTER COLUMN start_time DROP NOT NULL;

ALTER TABLE peerdb_stats.qrep_partitions
ALTER COLUMN start_time DROP NOT NULL;
