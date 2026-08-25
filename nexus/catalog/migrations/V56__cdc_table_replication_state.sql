CREATE TABLE IF NOT EXISTS cdc_table_replication_state (
    flow_name text NOT NULL,
    source_table_identifier text NOT NULL,
    cursor_text text NOT NULL DEFAULT '',
    last_attempt_at timestamptz,
    last_synced_at timestamptz,
    -- synced_batch_id/normalized_batch_id are this table's own sync/normalize
    -- progress, independent of every other table's; see per-table
    -- backpressure in flow/activities/flowable_isolated_cdc.go.
    synced_batch_id bigint NOT NULL DEFAULT 0,
    normalized_batch_id bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (flow_name, source_table_identifier)
);

-- Avro files staged by a table's sync step, pending normalize (INSERT straight
-- into the final destination table). One row per (table, synced_batch_id).
CREATE TABLE IF NOT EXISTS cdc_table_avro_stage (
    flow_name text NOT NULL,
    source_table_identifier text NOT NULL,
    batch_id bigint NOT NULL,
    avro_file jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (flow_name, source_table_identifier, batch_id)
);
