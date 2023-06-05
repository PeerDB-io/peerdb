ALTER TABLE flows
ADD COLUMN workflow_id TEXT;

ALTER TABLE flows
ADD COLUMN flow_status TEXT;

ALTER TABLE flows
ADD COLUMN flow_metadata JSONB;