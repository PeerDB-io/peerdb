-- Customizable ETL from PostgreSQL to Snowflake
CREATE MIRROR simple_mirror_from_src_to_dst FROM
	sql_server TO pg_flex FOR
$$
  SELECT * FROM peerdb_result
$$
WITH (
	destination_table_name = 'peerdb_stage.peerdb_result',
  initial_copy_only=true,
  disabled=true
);
