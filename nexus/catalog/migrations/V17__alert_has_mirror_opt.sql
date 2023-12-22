ALTER TABLE peerdb_state.alerts_v1
ADD COLUMN IF NOT EXISTS flow_name TEXT
ADD COLUMN IF NOT EXISTS ack BOOLEAN DEFAULT FALSE;

CREATE INDEX alerts_v1_flow_name_idx ON peerdb_state.alerts_v1 (flow_name);
