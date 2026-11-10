-- +goose NO TRANSACTION
-- +goose Up
DROP INDEX CONCURRENTLY IF EXISTS peerdb_stats.idx_cdc_batches_flow_name_end_time;
CREATE INDEX CONCURRENTLY idx_cdc_batches_flow_name_end_time ON peerdb_stats.cdc_batches (flow_name, end_time);
