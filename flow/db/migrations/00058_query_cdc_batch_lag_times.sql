-- +goose Up
ALTER TABLE query_cdc_avro_stage
    ADD COLUMN first_row_received_at timestamptz,
    ADD COLUMN first_row_commit_time timestamptz;

-- +goose Down
ALTER TABLE query_cdc_avro_stage
    DROP COLUMN first_row_received_at,
    DROP COLUMN first_row_commit_time;
