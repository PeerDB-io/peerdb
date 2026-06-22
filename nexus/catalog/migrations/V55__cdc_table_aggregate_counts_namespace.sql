-- Split the dotted destination_table_name into namespace + name for QualifiedTable
-- identifiers, same backfill semantics as V54.
ALTER TABLE peerdb_stats.cdc_table_aggregate_counts
ADD COLUMN IF NOT EXISTS destination_table_namespace TEXT NOT NULL DEFAULT '';

UPDATE peerdb_stats.cdc_table_aggregate_counts
SET destination_table_namespace = split_part(destination_table_name, '.', 1),
    destination_table_name = substr(destination_table_name, length(split_part(destination_table_name, '.', 1)) + 2)
WHERE position('.' in destination_table_name) > 0 AND destination_table_namespace = '';

ALTER TABLE peerdb_stats.cdc_table_aggregate_counts DROP CONSTRAINT cdc_table_aggregate_counts_pkey;
ALTER TABLE peerdb_stats.cdc_table_aggregate_counts
ADD PRIMARY KEY (flow_name, destination_table_namespace, destination_table_name);

DROP INDEX IF EXISTS peerdb_stats.cdc_table_aggregate_counts_dest_table_idx;
CREATE INDEX IF NOT EXISTS cdc_table_aggregate_counts_dest_table_idx
ON peerdb_stats.cdc_table_aggregate_counts (destination_table_namespace, destination_table_name);
