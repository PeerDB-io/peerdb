CREATE TABLE IF NOT EXISTS metadata_qrep_offloaded_partition_ranges (
    parent_mirror_name TEXT NOT NULL,
    run_uuid TEXT NOT NULL,
    partition_uuid TEXT NOT NULL,
    enc_key_id TEXT NOT NULL,
    range_payload BYTEA NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_uuid, partition_uuid)
);

CREATE INDEX IF NOT EXISTS idx_metadata_qrep_offloaded_partition_ranges_parent_mirror_name
    ON metadata_qrep_offloaded_partition_ranges (parent_mirror_name);
