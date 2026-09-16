-- +goose Up
ALTER TABLE query_cdc_replication_state
    ADD COLUMN IF NOT EXISTS inflight_state jsonb;

-- +goose Down
ALTER TABLE query_cdc_replication_state
    DROP COLUMN IF EXISTS inflight_state;
