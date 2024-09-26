CREATE TABLE IF NOT EXISTS table_schema_mapping (
    flow_name varchar(255) not null,
    table_name text not null,
    table_schema bytea not null,
    primary key (flow_name, table_name)
);
