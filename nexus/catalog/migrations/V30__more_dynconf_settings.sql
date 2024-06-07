INSERT INTO dynamic_settings (config_name,config_default_value,config_value_type,config_description,config_apply_mode)
VALUES
('PEERDB_CDC_CHANNEL_BUFFER_SIZE','262144',2,'Advanced setting: changes buffer size of channel PeerDB uses while streaming rows read to destination in CDC',1),
('PEERDB_QUEUE_FLUSH_TIMEOUT_SECONDS','10',2,'Frequency of flushing to queue, applicable for PeerDB Streams mirrors only',1),
('PEERDB_QUEUE_PARALLELISM','4',2,'Parallelism for Lua script processing data, applicable for CDC mirrors to Kakfa and PubSub',1),
('PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD','1000000',2,'CDC: number of records beyond which records are written to disk instead',1),
('PEERDB_CDC_DISK_SPILL_MEM_PERCENT_THRESHOLD','-1',2,'CDC: worker memory usage (in %) beyond which records are written to disk instead, -1 disables',1),
('PEERDB_ENABLE_WAL_HEARTBEAT','false',4,'enables WAL heartbeat to prevent replication slot lag from increasing during times of no activity',1),
('PEERDB_WAL_HEARTBEAT_QUERY','BEGIN;
DROP AGGREGATE IF EXISTS PEERDB_EPHEMERAL_HEARTBEAT(float4);
CREATE AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4) (SFUNC = float4pl, STYPE = float4);
DROP AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4);
END;',1,'SQL statement to run during each WAL heartbeat',1),
('PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE','false',4,'Advanced setting: enables experimental parallel sync (moving rows to target) and normalize (updating rows in target table)',2),
('PEERDB_SNOWFLAKE_MERGE_PARALLELISM','8',2,'Number of MERGE statements to run in parallel, applies to CDC mirrors with Snowflake targets. -1 means no limit',1),
('PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME','',1,'S3 buckets to store Avro files for mirrors with ClickHouse target',1),
('PEERDB_QUEUE_FORCE_TOPIC_CREATION','false',4,'Force auto topic creation in mirrors, applies to Kafka and PubSub mirrors',4)
 ON CONFLICT DO NOTHING;
