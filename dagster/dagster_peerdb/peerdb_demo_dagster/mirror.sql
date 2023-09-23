CREATE MIRROR simple_mirror_from_src_to_dst_raw_final_exec
FROM sqlserver_peer TO postgres_peer FOR
$$
EXECUTE udsGetAllMasterPoolDataPeerDB
SELECT * FROM  ##UnionMasterPool
GO
$$
WITH (
        initial_copy_only = true,
        disabled=true,
        mode = 'append',
        parallelism = 1,
        refresh_interval = 10,
        destination_table_name = 'peerdb_stage.raw_union_pool'
);