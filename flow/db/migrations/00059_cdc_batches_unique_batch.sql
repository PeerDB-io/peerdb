-- +goose NO TRANSACTION
-- +goose Up
DELETE FROM peerdb_stats.cdc_batches a USING peerdb_stats.cdc_batches b
WHERE a.flow_name = b.flow_name AND a.batch_id = b.batch_id AND a.id < b.id;
DROP INDEX CONCURRENTLY IF EXISTS peerdb_stats.idx_cdc_batches_flow_name_batch_id_unique;
CREATE UNIQUE INDEX CONCURRENTLY idx_cdc_batches_flow_name_batch_id_unique ON peerdb_stats.cdc_batches (flow_name, batch_id);
