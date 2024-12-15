ALTER TABLE metadata_last_sync_state ADD COLUMN IF NOT EXISTS last_text text NOT NULL DEFAULT '';

