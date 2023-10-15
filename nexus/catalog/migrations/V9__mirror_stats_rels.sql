-- For the cdc_batches, set batch_id as the primary key
ALTER TABLE peerdb_stats.cdc_batches
ADD COLUMN id SERIAL PRIMARY KEY;

-- add unique constraint on flow_name and batch_id
ALTER TABLE peerdb_stats.cdc_batches
ADD CONSTRAINT uq_cdc_batches_flow_batch UNIQUE (flow_name, batch_id);

-- add incrementing id column to cdc_batch_table, make this the primary key
ALTER TABLE peerdb_stats.cdc_batch_table
ADD COLUMN id SERIAL PRIMARY KEY;

-- For the qrep_runs table, set run_uuid as the primary key
ALTER TABLE peerdb_stats.qrep_runs
ADD CONSTRAINT pk_qrep_runs PRIMARY KEY (run_uuid);

-- For the qrep_partitions table, set partition_uuid as the primary key
ALTER TABLE peerdb_stats.qrep_partitions
ADD CONSTRAINT pk_qrep_partitions PRIMARY KEY (partition_uuid);


-- Foreign key for flow_name in cdc_batches
ALTER TABLE peerdb_stats.cdc_batches
ADD CONSTRAINT fk_cdc_batches_flow_name
FOREIGN KEY (flow_name) REFERENCES peerdb_stats.cdc_flows(flow_name) ON DELETE CASCADE;

-- Composite foreign key for flow_name and batch_id in cdc_batch_table
ALTER TABLE peerdb_stats.cdc_batch_table
ADD CONSTRAINT fk_cdc_batch_table_flow_batch
FOREIGN KEY (flow_name, batch_id) REFERENCES peerdb_stats.cdc_batches(flow_name, batch_id) ON DELETE CASCADE;

-- Foreign key for run_uuid in qrep_partitions
ALTER TABLE peerdb_stats.qrep_partitions
ADD CONSTRAINT fk_qrep_partitions_run_uuid
FOREIGN KEY (run_uuid) REFERENCES peerdb_stats.qrep_runs(run_uuid) ON DELETE CASCADE;
