-- Add new columns for WAL metrics
ALTER TABLE peerdb_stats.peer_slot_size
ADD COLUMN IF NOT EXISTS sent_lsn TEXT,
ADD COLUMN IF NOT EXISTS current_lsn TEXT,
ADD COLUMN IF NOT EXISTS restart_to_confirmed_mb FLOAT,
ADD COLUMN IF NOT EXISTS confirmed_to_sent_mb FLOAT,
ADD COLUMN IF NOT EXISTS sent_to_current_mb FLOAT,
ADD COLUMN IF NOT EXISTS safe_wal_size BIGINT,
ADD COLUMN IF NOT EXISTS slot_active BOOLEAN,
ADD COLUMN IF NOT EXISTS wait_event_type TEXT,
ADD COLUMN IF NOT EXISTS wait_event TEXT,
ADD COLUMN IF NOT EXISTS backend_state TEXT,
ADD COLUMN IF NOT EXISTS logical_decoding_work_mem_mb BIGINT,
ADD COLUMN IF NOT EXISTS logical_decoding_work_mem_pending_restart BOOLEAN,
ADD COLUMN IF NOT EXISTS stats_reset BIGINT,
ADD COLUMN IF NOT EXISTS spill_txns BIGINT,
ADD COLUMN IF NOT EXISTS spill_count BIGINT,
ADD COLUMN IF NOT EXISTS spill_bytes BIGINT;
