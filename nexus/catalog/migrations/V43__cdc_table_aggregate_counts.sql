-- Create a new table to store pre-aggregated counts per flow and table
CREATE TABLE IF NOT EXISTS peerdb_stats.cdc_table_aggregate_counts (
    flow_name TEXT NOT NULL,
    destination_table_name TEXT NOT NULL,
    inserts_count BIGINT NOT NULL DEFAULT 0,
    updates_count BIGINT NOT NULL DEFAULT 0,
    deletes_count BIGINT NOT NULL DEFAULT 0,
    total_count BIGINT NOT NULL DEFAULT 0,
    latest_batch_id BIGINT NOT NULL DEFAULT 0,
    last_updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (flow_name, destination_table_name)
);

-- Create an index to speed up lookups by flow_name
CREATE INDEX IF NOT EXISTS cdc_table_aggregate_counts_flow_name_idx 
ON peerdb_stats.cdc_table_aggregate_counts (flow_name);

-- Create an index to speed up lookups by destination_table_name
CREATE INDEX IF NOT EXISTS cdc_table_aggregate_counts_dest_table_idx
ON peerdb_stats.cdc_table_aggregate_counts (destination_table_name);

-- Create an index to speed up lookups by last_updated_at
CREATE INDEX IF NOT EXISTS cdc_table_aggregate_counts_last_updated_idx
ON peerdb_stats.cdc_table_aggregate_counts (last_updated_at);

-- Initial data population from existing data
INSERT INTO peerdb_stats.cdc_table_aggregate_counts (
    flow_name, 
    destination_table_name, 
    inserts_count, 
    updates_count, 
    deletes_count, 
    total_count,
    latest_batch_id
)
SELECT 
    t.flow_name,
    t.destination_table_name,
    COALESCE(SUM(t.insert_count), 0) as inserts_count,
    COALESCE(SUM(t.update_count), 0) as updates_count,
    COALESCE(SUM(t.delete_count), 0) as deletes_count,
    COALESCE(SUM(t.insert_count), 0) + COALESCE(SUM(t.update_count), 0) + COALESCE(SUM(t.delete_count), 0) as total_count,
    COALESCE(MAX(t.batch_id), 0) as latest_batch_id
FROM 
    peerdb_stats.cdc_batch_table t
GROUP BY 
    t.flow_name, t.destination_table_name
ON CONFLICT (flow_name, destination_table_name) DO UPDATE SET
    inserts_count = EXCLUDED.inserts_count,
    updates_count = EXCLUDED.updates_count,
    deletes_count = EXCLUDED.deletes_count,
    total_count = EXCLUDED.total_count,
    latest_batch_id = EXCLUDED.latest_batch_id,
    last_updated_at = NOW();

-- Add comment explaining the purpose of the table
COMMENT ON TABLE peerdb_stats.cdc_table_aggregate_counts IS 
'Stores pre-aggregated counts of CDC operations by flow and table to optimize query performance.';