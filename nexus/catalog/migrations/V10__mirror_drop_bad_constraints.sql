-- Drop the foreign key constraint from qrep_partitions to qrep_runs
ALTER TABLE peerdb_stats.qrep_partitions
DROP CONSTRAINT fk_qrep_partitions_run_uuid;

-- Drop the unique constraint for flow_name from qrep_runs
ALTER TABLE peerdb_stats.qrep_runs
DROP CONSTRAINT uq_qrep_runs_flow_name;

-- Add unique constraint to qrep_runs for (flow_name, run_uuid)
ALTER TABLE peerdb_stats.qrep_runs
ADD CONSTRAINT uq_qrep_runs_flow_run
UNIQUE (flow_name, run_uuid);

-- Add foreign key from qrep_partitions to qrep_runs
ALTER TABLE peerdb_stats.qrep_partitions
ADD CONSTRAINT fk_qrep_partitions_run
FOREIGN KEY (flow_name, run_uuid)
REFERENCES peerdb_stats.qrep_runs(flow_name, run_uuid)
ON DELETE CASCADE;
