-- Customizable ETL from PostgreSQL to Snowflake
CREATE MIRROR simple_mirror_from_src_to_dst FROM
	postgres_peer TO postgres_peer FOR
$$
  SELECT * FROM src.events
$$
WITH (
	destination_table_name = 'dst.events',
	parallelism = 2,
	refresh_interval = 30,
  initial_copy_only=true,
  disabled=true
);
