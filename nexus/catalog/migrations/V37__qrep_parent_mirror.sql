ALTER TABLE peerdb_stats.qrep_partitions ADD COLUMN IF NOT EXISTS parent_mirror_name TEXT;
ALTER TABLE peerdb_stats.qrep_runs ADD COLUMN IF NOT EXISTS parent_mirror_name TEXT;
