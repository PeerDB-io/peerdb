create table v2cdc (
    flow_name text,
    xid xid,
    lsn pg_lsn,
    stream bytea[],
    primary key (flow_name, xid, lsn)
);
