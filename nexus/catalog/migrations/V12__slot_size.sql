CREATE TABLE IF NOT EXISTS peerdb_stats.peer_slot_size (
    slot_name TEXT NOT NULL,
    peer_name TEXT NOT NULL,
    redo_lsn TEXT,
    restart_lsn TEXT,
    confirmed_flush_lsn TEXT,
    slot_size BIGINT,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
