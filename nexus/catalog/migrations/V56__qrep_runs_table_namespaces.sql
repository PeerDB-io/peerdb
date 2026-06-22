-- Namespace columns for QualifiedTable identifiers; source_table/destination_table
-- keep holding the table component only for new rows, old rows are split here.
ALTER TABLE peerdb_stats.qrep_runs ADD COLUMN IF NOT EXISTS source_table_namespace TEXT NOT NULL DEFAULT '';
ALTER TABLE peerdb_stats.qrep_runs ADD COLUMN IF NOT EXISTS destination_table_namespace TEXT NOT NULL DEFAULT '';

UPDATE peerdb_stats.qrep_runs
SET source_table_namespace = split_part(source_table, '.', 1),
    source_table = substr(source_table, length(split_part(source_table, '.', 1)) + 2)
WHERE source_table IS NOT NULL AND position('.' in source_table) > 0 AND source_table_namespace = '';

UPDATE peerdb_stats.qrep_runs
SET destination_table_namespace = split_part(destination_table, '.', 1),
    destination_table = substr(destination_table, length(split_part(destination_table, '.', 1)) + 2)
WHERE destination_table IS NOT NULL AND position('.' in destination_table) > 0 AND destination_table_namespace = '';
