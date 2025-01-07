ALTER TABLE peerdb_stats.schema_deltas_audit_log ADD COLUMN IF NOT EXISTS batch_id BIGINT;
