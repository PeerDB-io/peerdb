ALTER TABLE ch_s3_stage ADD CONSTRAINT 
ch_s3_stage_flow_job_name_sync_batch_id_key 
UNIQUE (flow_job_name, sync_batch_id);
