CREATE TABLE IF NOT EXISTS table_mappings (
    flow_name varchar(255) not null,
    version bigint not null default 1,
    table_mappings bytea[] not null,
    json_blob jsonb,
    primary key (flow_name, version)
);
