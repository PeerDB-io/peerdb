CREATE TABLE IF NOT EXISTS ch_s3_stage(
    id SERIAL PRIMARY KEY,
    flow_job_name TEXT NOT NULL,
    sync_batch_id BIGINT NOT NULL,
    avro_file JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ch_s3_stage_flow_job_name_idx ON ch_s3_stage (flow_job_name);

CREATE INDEX IF NOT EXISTS ch_s3_stage_sync_batch_id_idx ON ch_s3_stage (sync_batch_id);
