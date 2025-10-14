CREATE TABLE IF NOT EXISTS table_mappings (
    flow_name varchar(255) not null,Expand commentComment on line R2ResolvedCode has comments. Press enter to view.
    version int32 not null default 1,
    table_mappings []bytea not null,
    primary key (flow_name, version)
);
