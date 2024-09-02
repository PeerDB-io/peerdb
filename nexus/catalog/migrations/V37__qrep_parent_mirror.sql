ALTER TABLE qrep_partitions ADD COLUMN IF NOT EXISTS parent_mirror_name TEXT;
ALTER TABLE qrep_runs ADD COLUMN IF NOT EXISTS parent_mirror_name TEXT;
