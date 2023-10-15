-- For the cdc_batches, set batch_id as the primary key
ALTER TABLE peerdb_stats.cdc_batches
ADD COLUMN id SERIAL PRIMARY KEY;

-- add incrementing id column to cdc_batch_table, make this the primary key
ALTER TABLE peerdb_stats.cdc_batch_table
ADD COLUMN id SERIAL PRIMARY KEY;

-- add incrementing id column to qrep_runs, make this the primary key
ALTER TABLE peerdb_stats.qrep_runs
ADD COLUMN id SERIAL PRIMARY KEY;

-- add unique for flow_name to qrep_runs
ALTER TABLE peerdb_stats.qrep_runs
ADD CONSTRAINT uq_qrep_runs_flow_name
UNIQUE (flow_name);

-- add incrementing id column to qrep_partitions, make this the primary key
ALTER TABLE peerdb_stats.qrep_partitions
ADD COLUMN id SERIAL PRIMARY KEY;

-- For peerdb_stats.cdc_batches
CREATE INDEX idx_cdc_batches_flow_name ON peerdb_stats.cdc_batches USING HASH(flow_name);
CREATE INDEX idx_cdc_batches_batch_id ON peerdb_stats.cdc_batches(batch_id);
CREATE INDEX idx_cdc_batches_start_time ON peerdb_stats.cdc_batches(start_time);

-- For peerdb_stats.cdc_batch_table
CREATE INDEX idx_cdc_batch_table_flow_name_batch_id ON peerdb_stats.cdc_batch_table(flow_name, batch_id);

-- For peerdb_stats.qrep_runs
CREATE INDEX idx_qrep_runs_flow_name ON peerdb_stats.qrep_runs USING HASH(flow_name);
CREATE INDEX idx_qrep_runs_run_uuid ON peerdb_stats.qrep_runs USING HASH(run_uuid);
CREATE INDEX idx_qrep_runs_start_time ON peerdb_stats.qrep_runs(start_time);

-- For peerdb_stats.qrep_partitions
CREATE INDEX idx_qrep_partitions_flow_name_run_uuid ON peerdb_stats.qrep_partitions(flow_name, run_uuid);
CREATE INDEX idx_qrep_partitions_partition_uuid ON peerdb_stats.qrep_partitions USING HASH(partition_uuid);
CREATE INDEX idx_qrep_partitions_start_time ON peerdb_stats.qrep_partitions(start_time);

-- add fkey from cdc_batches to cdc_flows
ALTER TABLE peerdb_stats.cdc_batches
ADD CONSTRAINT fk_cdc_batches_flow_name
FOREIGN KEY (flow_name)
REFERENCES peerdb_stats.cdc_flows (flow_name)
ON DELETE CASCADE;

-- add fkey from cdc_batch_table to cdc_flows
ALTER TABLE peerdb_stats.cdc_batch_table
ADD CONSTRAINT fk_cdc_batch_table_flow_name
FOREIGN KEY (flow_name)
REFERENCES peerdb_stats.cdc_flows (flow_name)
ON DELETE CASCADE;

-- add fkey from qrep_partitions to qrep_runs
ALTER TABLE peerdb_stats.qrep_partitions
ADD CONSTRAINT fk_qrep_partitions_run_uuid
FOREIGN KEY (flow_name)
REFERENCES peerdb_stats.qrep_runs (flow_name)
ON DELETE CASCADE;
