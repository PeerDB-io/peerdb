ALTER TABLE flows
ADD COLUMN workflow_id TEXT
ADD COLUMN flow_status TEXT
ADD COLUMN flow_metadata JSONB;