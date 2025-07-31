create function utc_now() returns timestamp as $$
  select now() at time zone 'utc';
$$ language sql;

CREATE TABLE IF NOT EXISTS peerdb_stats.snapshots (
    flow_name TEXT NOT NULL,
    snapshot_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS peerdb_stats.granular_status (
    -- ideally a foreign key but flows.name is not guaranteed to be unique
    flow_name text PRIMARY KEY,
    snapshot_current_id INTEGER,
    snapshot_succeeding BOOLEAN NOT NULL,
    snapshot_failing_qrep_run_ids TEXT[] NOT NULL DEFAULT '{}',
    snapshot_failing_partition_ids TEXT[] NOT NULL DEFAULT '{}',
    snapshot_is_internal_error BOOLEAN NOT NULL DEFAULT false,
    snapshot_updated_at TIMESTAMP NOT NULL DEFAULT utc_now(),
    sync_succeeding BOOLEAN NOT NULL,
    sync_is_internal_error BOOLEAN NOT NULL DEFAULT false,
    sync_last_successful_batch_id BIGINT,
    sync_updated_at TIMESTAMP NOT NULL DEFAULT utc_now(),
    normalize_succeeding BOOLEAN NOT NULL,
    normalize_is_internal_error BOOLEAN NOT NULL DEFAULT false,
    normalize_last_successful_batch_id BIGINT,
    normalize_updated_at TIMESTAMP NOT NULL DEFAULT utc_now(),
    slot_lag_low BOOLEAN NOT NULL,
    slot_lag_mib FLOAT,
    slot_lag_updated_at TIMESTAMP NOT NULL DEFAULT utc_now()
);
