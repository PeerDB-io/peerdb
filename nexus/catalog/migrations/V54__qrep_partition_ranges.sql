-- Stores QRep partition ranges offloaded to the catalog instead of being passed
-- through Temporal. Today this is used only for potentially sensitive ranges
-- (non-UUID strings), which are encrypted and kept out of Temporal payloads.
-- The table name and columns are deliberately generic so a future change can offload
-- all partition ranges here to stay under Temporal's payload size limit, not just
-- the sensitive ones.
CREATE TABLE IF NOT EXISTS metadata_qrep_partition_ranges (
    flow_name TEXT NOT NULL,
    run_uuid TEXT NOT NULL,
    partition_uuid TEXT NOT NULL,
    enc_key_id TEXT NOT NULL,
    range_payload BYTEA NOT NULL,
    PRIMARY KEY (run_uuid, partition_uuid)
);

CREATE INDEX IF NOT EXISTS idx_metadata_qrep_partition_ranges_flow_name ON metadata_qrep_partition_ranges (flow_name);
