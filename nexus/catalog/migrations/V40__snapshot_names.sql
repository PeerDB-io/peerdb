CREATE TABLE IF NOT EXISTS snapshot_names (
    flow_name varchar(255) primary key,
    slot_name text not null,
    snapshot_name text not null,
    supports_tid_scan bool not null
);

