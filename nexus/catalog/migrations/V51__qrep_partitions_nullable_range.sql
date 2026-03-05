ALTER TABLE peerdb_stats.qrep_partitions
ALTER COLUMN partition_start DROP NOT NULL;

ALTER TABLE peerdb_stats.qrep_partitions
ALTER COLUMN partition_end DROP NOT NULL;
