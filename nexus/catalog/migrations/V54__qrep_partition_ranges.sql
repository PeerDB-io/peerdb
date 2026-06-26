-- Stores QRep partition ranges offloaded to the catalog instead of being passed
-- through Temporal. Today this is used only for sensitive partition ranges.
-- The table name and columns are deliberately generic so a future change can
-- offload all partition ranges to catalog to stay under Temporal's payload limit.
CREATE TABLE IF NOT EXISTS metadata_qrep_partition_ranges (
    parent_mirror_name TEXT NOT NULL,
    run_uuid TEXT NOT NULL,
    partition_uuid TEXT NOT NULL,
    enc_key_id TEXT NOT NULL,
    range_payload BYTEA NOT NULL,
    PRIMARY KEY (run_uuid, partition_uuid)
);

CREATE INDEX IF NOT EXISTS idx_metadata_qrep_partition_ranges_parent_mirror_name
    ON metadata_qrep_partition_ranges (parent_mirror_name);
