-- +goose Up
CREATE TABLE IF NOT EXISTS query_cdc_replication_state (
    flow_name text NOT NULL,
    source_table_identifier text NOT NULL,
    cursor_text text NOT NULL DEFAULT '',
    last_attempt_at timestamptz,
    last_synced_at timestamptz,
    -- synced_batch_id/normalized_batch_id are this table's own sync/normalize
    -- progress, independent of every other table's; see per-table
    -- backpressure in flow/activities/flowable_query_cdc.go.
    synced_batch_id bigint NOT NULL DEFAULT 0,
    normalized_batch_id bigint NOT NULL DEFAULT 0,
    last_normalized_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    inserts_count bigint NOT NULL DEFAULT 0,
    updates_count bigint NOT NULL DEFAULT 0,
    deletes_count bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (flow_name, source_table_identifier)
);

-- Avro files staged by a table's sync step, pending normalize (INSERT straight
-- into the final destination table). One row per (table, synced_batch_id).
CREATE TABLE IF NOT EXISTS query_cdc_avro_stage (
    flow_name text NOT NULL,
    source_table_identifier text NOT NULL,
    -- per-table batch_id, matching query_cdc_replication_state.synced_batch_id /
    -- normalized_batch_id - not the global batch_id used in the cdc_batches table.
    batch_id bigint NOT NULL,
    avro_file jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    -- insert/update/delete counts for this batch
    inserts_count bigint NOT NULL DEFAULT 0,
    updates_count bigint NOT NULL DEFAULT 0,
    deletes_count bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (flow_name, source_table_identifier, batch_id)
);
