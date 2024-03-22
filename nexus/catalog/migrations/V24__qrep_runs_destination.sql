ALTER TABLE peerdb_stats.qrep_runs
ADD COLUMN destination_table TEXT;

ALTER TABLE peerdb_stats.qrep_runs
ADD COLUMN source_table TEXT;

ALTER TABLE peerdb_stats.qrep_runs
ADD COLUMN fetch_complete BOOLEAN DEFAULT FALSE;

ALTER TABLE peerdb_stats.qrep_runs
ADD COLUMN consolidate_complete BOOLEAN DEFAULT FALSE;
