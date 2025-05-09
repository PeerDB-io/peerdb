package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/constraints"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	DefaultPeerDBS3PartSize int64 = 50 * 1024 * 1024 // 50MiB
)

var DynamicSettings = [...]*protos.DynamicSetting{
	{
		Name:             "PEERDB_CDC_CHANNEL_BUFFER_SIZE",
		Description:      "Advanced setting: changes buffer size of channel PeerDB uses while streaming rows read to destination in CDC",
		DefaultValue:     "262144",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name: "PEERDB_NORMALIZE_CHANNEL_BUFFER_SIZE",
		Description: "Advanced setting: changes buffer size of channel PeerDB uses for queueing normalizing, " +
			"use with PEERDB_PARALLEL_SYNC_NORMALIZE",
		DefaultValue:     "128",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_AFTER_RESUME,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_QUEUE_FLUSH_TIMEOUT_SECONDS",
		Description:      "Frequency of flushing to queue, applicable for PeerDB Streams mirrors only",
		DefaultValue:     "10",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_QUEUES,
	},
	{
		Name:             "PEERDB_QUEUE_PARALLELISM",
		Description:      "Parallelism for Lua script processing data, applicable for CDC mirrors to Kakfa and PubSub",
		DefaultValue:     "4",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_QUEUES,
	},
	{
		Name:             "PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD",
		Description:      "CDC: number of records beyond which records are written to disk instead",
		DefaultValue:     "1000000",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_CDC_DISK_SPILL_MEM_PERCENT_THRESHOLD",
		Description:      "CDC: worker memory usage (in %) beyond which records are written to disk instead, -1 disables",
		DefaultValue:     "-1",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_ENABLE_WAL_HEARTBEAT",
		Description:      "Enables WAL heartbeat to prevent replication slot lag from increasing during times of no activity",
		DefaultValue:     "true",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_WAL_HEARTBEAT_QUERY",
		DefaultValue:     "SELECT pg_logical_emit_message(false,'peerdb_heartbeat','')",
		ValueType:        protos.DynconfValueType_STRING,
		Description:      "SQL to run during each WAL heartbeat",
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE",
		Description:      "Enables parallel sync (moving rows to target) and normalize (updating rows in target table)",
		DefaultValue:     "true",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_AFTER_RESUME,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_RECONNECT_AFTER_BATCHES",
		Description:      "Force peerdb to reconnect connection to source after N batches",
		DefaultValue:     "0",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_AFTER_RESUME,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:         "PEERDB_FULL_REFRESH_OVERWRITE_MODE",
		Description:  "Enables full refresh mode for query replication mirrors of overwrite type",
		DefaultValue: "false",
		ValueType:    protos.DynconfValueType_BOOL,
		ApplyMode:    protos.DynconfApplyMode_APPLY_MODE_NEW_MIRROR,
	},
	{
		Name:             "PEERDB_NULLABLE",
		Description:      "Propagate nullability in schema",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_NEW_MIRROR,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_CLICKHOUSE_BINARY_FORMAT",
		Description:      "Binary field encoding on clickhouse destination; either raw, hex, or base64",
		DefaultValue:     "raw",
		ValueType:        protos.DynconfValueType_STRING,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_AFTER_RESUME,
		TargetForSetting: protos.DynconfTarget_CLICKHOUSE,
	},
	{
		Name:             "PEERDB_SNOWFLAKE_MERGE_PARALLELISM",
		Description:      "Parallel MERGE statements to run for CDC mirrors with Snowflake targets. -1 for no limit",
		DefaultValue:     "8",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_SNOWFLAKE,
	},
	{
		Name:             "PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME",
		Description:      "S3 buckets to store Avro files for mirrors with ClickHouse target",
		DefaultValue:     "",
		ValueType:        protos.DynconfValueType_STRING,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_CLICKHOUSE,
	},
	{
		Name: "PEERDB_S3_PART_SIZE",
		Description: "S3 upload part size in bytes, may need to increase for large batches. " +
			"https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html",
		DefaultValue:     strconv.FormatInt(DefaultPeerDBS3PartSize, 10),
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_QUEUE_FORCE_TOPIC_CREATION",
		Description:      "Force auto topic creation in mirrors, applies to Kafka and PubSub mirrors",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_NEW_MIRROR,
		TargetForSetting: protos.DynconfTarget_QUEUES,
	},
	{
		Name:             "PEERDB_ALERTING_GAP_MINUTES",
		Description:      "Duration in minutes before reraising alerts, 0 disables all alerting entirely",
		DefaultValue:     "15",
		ValueType:        protos.DynconfValueType_UINT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD",
		Description:      "Lag (in MB) threshold on PeerDB slot to start sending alerts, 0 disables slot lag alerting entirely",
		DefaultValue:     "5000",
		ValueType:        protos.DynconfValueType_UINT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD",
		Description:      "Open connections from PeerDB user threshold to start sending alerts, 0 disables open connections alerting entirely",
		DefaultValue:     "5",
		ValueType:        protos.DynconfValueType_UINT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_BIGQUERY_ENABLE_SYNCED_AT_PARTITIONING_BY_DAYS",
		Description:      "BigQuery only: create target tables with partitioning by _PEERDB_SYNCED_AT column",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_NEW_MIRROR,
		TargetForSetting: protos.DynconfTarget_BIGQUERY,
	},
	{
		Name: "PEERDB_BIGQUERY_TOAST_MERGE_CHUNKING",
		Description: "BigQuery only: controls number of unchanged toast columns merged per statement in normalization. " +
			"Avoids statements growing too large",
		DefaultValue:     "8",
		ValueType:        protos.DynconfValueType_UINT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_AFTER_RESUME,
		TargetForSetting: protos.DynconfTarget_BIGQUERY,
	},
	{
		Name:             "PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE",
		Description:      "Enable generating deletion records for updates in ClickHouse, avoids stale records when primary key updated",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_CLICKHOUSE,
	},
	{
		Name:             "PEERDB_CLICKHOUSE_MAX_INSERT_THREADS",
		Description:      "Configures max_insert_threads setting on clickhouse for inserting into destination table. Setting left unset when 0",
		DefaultValue:     "0",
		ValueType:        protos.DynconfValueType_UINT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_CLICKHOUSE,
	},
	{
		Name:             "PEERDB_CLICKHOUSE_PARALLEL_NORMALIZE",
		Description:      "Divide tables in batch into N insert selects. Helps distribute load to multiple nodes",
		DefaultValue:     "0",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_CLICKHOUSE,
	},
	{
		Name:             "PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING",
		Description:      "Map unbounded numerics in Postgres to String in ClickHouse to preserve precision and scale",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_NEW_MIRROR,
		TargetForSetting: protos.DynconfTarget_CLICKHOUSE,
	},
	{
		Name:             "PEERDB_INTERVAL_SINCE_LAST_NORMALIZE_THRESHOLD_MINUTES",
		Description:      "Duration in minutes since last normalize to start alerting, 0 disables all alerting entirely",
		DefaultValue:     "240",
		ValueType:        protos.DynconfValueType_UINT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_APPLICATION_NAME_PER_MIRROR_NAME",
		Description:      "Set Postgres application_name to have mirror name as suffix for each mirror",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_MAINTENANCE_MODE_ENABLED",
		Description:      "Whether PeerDB is in maintenance mode, which disables any modifications to mirrors",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name: "PEERDB_PKM_EMPTY_BATCH_THROTTLE_THRESHOLD_SECONDS",
		Description: "Throttle threshold seconds for always sending KeepAlive response when no records are processed, " +
			"-1 disables always sending responses when no records are processed",
		DefaultValue:     "60",
		ValueType:        protos.DynconfValueType_INT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_AFTER_RESUME,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name:             "PEERDB_CLICKHOUSE_NORMALIZATION_PARTS",
		Description:      "Chunk normalization into N queries, can help mitigate OOM issues on ClickHouse",
		DefaultValue:     "1",
		ValueType:        protos.DynconfValueType_UINT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_AFTER_RESUME,
		TargetForSetting: protos.DynconfTarget_CLICKHOUSE,
	},
	{
		Name:             "PEERDB_CLICKHOUSE_INITIAL_LOAD_PARTS_PER_PARTITION",
		Description:      "Chunk partitions in initial load into N queries, can help mitigate OOM issues on ClickHouse",
		DefaultValue:     "1",
		ValueType:        protos.DynconfValueType_UINT,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_AFTER_RESUME,
		TargetForSetting: protos.DynconfTarget_CLICKHOUSE,
	},
	{
		Name:             "PEERDB_SKIP_SNAPSHOT_EXPORT",
		Description:      "This avoids initial load failing due to connectivity drops, but risks data consistency unless precautions are taken",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_NEW_MIRROR,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name: "PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN",
		Description: "Ingest source schema as column to destination. " +
			"Useful when multiple tables from source ingest into single table on destination",
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_NEW_MIRROR,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
	{
		Name: "PEERDB_POSTGRES_CDC_HANDLE_INHERITANCE_FOR_NON_PARTITIONED_TABLES",
		Description: "For Postgres CDC: attempt to fetch/remap child tables for tables that aren't partitioned by Postgres." +
			"Useful for tables that are partitioned by extensions or table inheritance",
		DefaultValue:     "true",
		ValueType:        protos.DynconfValueType_BOOL,
		ApplyMode:        protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE,
		TargetForSetting: protos.DynconfTarget_ALL,
	},
}

var DynamicIndex = func() map[string]int {
	defaults := make(map[string]int, len(DynamicSettings))
	for i, setting := range DynamicSettings {
		defaults[setting.Name] = i
	}
	return defaults
}()

type BinaryFormat int

const (
	BinaryFormatInvalid = iota
	BinaryFormatRaw
	BinaryFormatBase64
	BinaryFormatHex
)

func dynLookup(ctx context.Context, env map[string]string, key string) (string, error) {
	if val, ok := env[key]; ok {
		return val, nil
	}

	conn, err := GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		LoggerFromCtx(ctx).Error("Failed to get catalog connection pool", slog.Any("error", err))
		return "", fmt.Errorf("failed to get catalog connection pool: %w", err)
	}

	var setting *protos.DynamicSetting
	if idx, ok := DynamicIndex[key]; ok {
		setting = DynamicSettings[idx]
	}

	var value pgtype.Text
	query := "SELECT config_value FROM dynamic_settings WHERE config_name=$1"
	if err := conn.QueryRow(ctx, query, key).Scan(&value); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		LoggerFromCtx(ctx).Error("Failed to get key", slog.Any("error", err))
		return "", fmt.Errorf("failed to get key: %w", err)
	}
	if !value.Valid {
		if val, ok := os.LookupEnv(key); ok {
			if env != nil && setting != nil && setting.ApplyMode != protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE {
				env[key] = val
			}
			return val, nil
		}
		if setting != nil {
			if env != nil && setting.ApplyMode != protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE {
				env[key] = setting.DefaultValue
			}
			return setting.DefaultValue, nil
		}
	}
	if env != nil && setting != nil && setting.ApplyMode != protos.DynconfApplyMode_APPLY_MODE_IMMEDIATE {
		env[key] = value.String
	}
	return value.String, nil
}

func dynLookupConvert[T any](ctx context.Context, env map[string]string, key string, fn func(string) (T, error)) (T, error) {
	value, err := dynLookup(ctx, env, key)
	if err != nil {
		var none T
		return none, err
	}
	return fn(value)
}

func dynamicConfSigned[T constraints.Signed](ctx context.Context, env map[string]string, key string) (T, error) {
	value, err := dynLookupConvert(ctx, env, key, func(value string) (int64, error) {
		return strconv.ParseInt(value, 10, 64)
	})
	if err != nil {
		LoggerFromCtx(ctx).Error("Failed to parse as int64", slog.String("key", key), slog.Any("error", err))
		return 0, fmt.Errorf("failed to parse %s as int64: %w", key, err)
	}

	return T(value), nil
}

func dynamicConfUnsigned[T constraints.Unsigned](ctx context.Context, env map[string]string, key string) (T, error) {
	value, err := dynLookupConvert(ctx, env, key, func(value string) (uint64, error) {
		return strconv.ParseUint(value, 10, 64)
	})
	if err != nil {
		LoggerFromCtx(ctx).Error("Failed to parse as uint64", slog.String("key", key), slog.Any("error", err))
		return 0, fmt.Errorf("failed to parse %s as uint64: %w", key, err)
	}

	return T(value), nil
}

func dynamicConfBool(ctx context.Context, env map[string]string, key string) (bool, error) {
	value, err := dynLookupConvert(ctx, env, key, strconv.ParseBool)
	if err != nil {
		LoggerFromCtx(ctx).Error("Failed to parse bool", slog.String("key", key), slog.Any("error", err))
		return false, fmt.Errorf("failed to parse %s as bool: %w", key, err)
	}

	return value, nil
}

func UpdateDynamicSetting(ctx context.Context, pool shared.CatalogPool, name string, value *string) error {
	if pool.Pool == nil {
		var err error
		pool, err = GetCatalogConnectionPoolFromEnv(ctx)
		if err != nil {
			LoggerFromCtx(ctx).Error("Failed to get catalog connection pool for dynamic setting update", slog.Any("error", err))
			return fmt.Errorf("failed to get catalog connection pool: %w", err)
		}
	}
	_, err := pool.Exec(ctx, `insert into dynamic_settings (config_name, config_value) values ($1, $2)
			on conflict (config_name) do update set config_value = $2`, name, value)
	return err
}

// PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD, 0 disables slot lag alerting entirely
func PeerDBSlotLagMBAlertThreshold(ctx context.Context, env map[string]string) (uint32, error) {
	return dynamicConfUnsigned[uint32](ctx, env, "PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD")
}

// PEERDB_ALERTING_GAP_MINUTES, 0 disables all alerting entirely
func PeerDBAlertingGapMinutesAsDuration(ctx context.Context, env map[string]string) (time.Duration, error) {
	why, err := dynamicConfSigned[int64](ctx, env, "PEERDB_ALERTING_GAP_MINUTES")
	if err != nil {
		return 0, err
	}
	return time.Duration(why) * time.Minute, nil
}

// PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD, 0 disables open connections alerting entirely
func PeerDBOpenConnectionsAlertThreshold(ctx context.Context, env map[string]string) (uint32, error) {
	return dynamicConfUnsigned[uint32](ctx, env, "PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD")
}

// PEERDB_BIGQUERY_ENABLE_SYNCED_AT_PARTITIONING_BY_DAYS, for creating target tables with
// partitioning by _PEERDB_SYNCED_AT column
// If true, the target tables will be partitioned by _PEERDB_SYNCED_AT column
// If false, the target tables will not be partitioned
func PeerDBBigQueryEnableSyncedAtPartitioning(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_BIGQUERY_ENABLE_SYNCED_AT_PARTITIONING_BY_DAYS")
}

func PeerDBBigQueryToastMergeChunking(ctx context.Context, env map[string]string) (uint32, error) {
	return dynamicConfUnsigned[uint32](ctx, env, "PEERDB_BIGQUERY_TOAST_MERGE_CHUNKING")
}

func PeerDBCDCChannelBufferSize(ctx context.Context, env map[string]string) (int, error) {
	return dynamicConfSigned[int](ctx, env, "PEERDB_CDC_CHANNEL_BUFFER_SIZE")
}

func PeerDBNormalizeChannelBufferSize(ctx context.Context, env map[string]string) (int, error) {
	return dynamicConfSigned[int](ctx, env, "PEERDB_NORMALIZE_CHANNEL_BUFFER_SIZE")
}

func PeerDBQueueFlushTimeoutSeconds(ctx context.Context, env map[string]string) (time.Duration, error) {
	x, err := dynamicConfSigned[int64](ctx, env, "PEERDB_QUEUE_FLUSH_TIMEOUT_SECONDS")
	if err != nil {
		return 0, err
	}
	return time.Duration(x) * time.Second, nil
}

func PeerDBQueueParallelism(ctx context.Context, env map[string]string) (int64, error) {
	return dynamicConfSigned[int64](ctx, env, "PEERDB_QUEUE_PARALLELISM")
}

func PeerDBCDCDiskSpillRecordsThreshold(ctx context.Context, env map[string]string) (int64, error) {
	return dynamicConfSigned[int64](ctx, env, "PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD")
}

func PeerDBCDCDiskSpillMemPercentThreshold(ctx context.Context, env map[string]string) (int64, error) {
	return dynamicConfSigned[int64](ctx, env, "PEERDB_CDC_DISK_SPILL_MEM_PERCENT_THRESHOLD")
}

func PeerDBEnableWALHeartbeat(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_ENABLE_WAL_HEARTBEAT")
}

func PeerDBWALHeartbeatQuery(ctx context.Context, env map[string]string) (string, error) {
	return dynLookup(ctx, env, "PEERDB_WAL_HEARTBEAT_QUERY")
}

func PeerDBEnableParallelSyncNormalize(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE")
}

func PeerDBReconnectAfterBatches(ctx context.Context, env map[string]string) (int32, error) {
	return dynamicConfSigned[int32](ctx, env, "PEERDB_RECONNECT_AFTER_BATCHES")
}

func PeerDBFullRefreshOverwriteMode(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_FULL_REFRESH_OVERWRITE_MODE")
}

func PeerDBNullable(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_NULLABLE")
}

func PeerDBBinaryFormat(ctx context.Context, env map[string]string) (BinaryFormat, error) {
	format, err := dynLookup(ctx, env, "PEERDB_CLICKHOUSE_BINARY_FORMAT")
	if err != nil {
		return 0, err
	}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "raw":
		return BinaryFormatRaw, nil
	case "hex":
		return BinaryFormatHex, nil
	case "base64":
		return BinaryFormatBase64, nil
	default:
		return 0, fmt.Errorf("unknown binary format %s", format)
	}
}

func PeerDBEnableClickHousePrimaryUpdate(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE")
}

func PeerDBClickHouseMaxInsertThreads(ctx context.Context, env map[string]string) (int64, error) {
	return dynamicConfSigned[int64](ctx, env, "PEERDB_CLICKHOUSE_MAX_INSERT_THREADS")
}

func PeerDBClickHouseParallelNormalize(ctx context.Context, env map[string]string) (int, error) {
	return dynamicConfSigned[int](ctx, env, "PEERDB_CLICKHOUSE_PARALLEL_NORMALIZE")
}

func PeerDBEnableClickHouseNumericAsString(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING")
}

func PeerDBSnowflakeMergeParallelism(ctx context.Context, env map[string]string) (int64, error) {
	return dynamicConfSigned[int64](ctx, env, "PEERDB_SNOWFLAKE_MERGE_PARALLELISM")
}

func PeerDBClickHouseAWSS3BucketName(ctx context.Context, env map[string]string) (string, error) {
	return dynLookup(ctx, env, "PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME")
}

func PeerDBS3PartSize(ctx context.Context, env map[string]string) (int64, error) {
	return dynamicConfSigned[int64](ctx, env, "PEERDB_S3_PART_SIZE")
}

// Kafka has topic auto create as an option, auto.create.topics.enable
// But non-dedicated cluster maybe can't set config, may want peerdb to create topic. Similar for PubSub
func PeerDBQueueForceTopicCreation(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_QUEUE_FORCE_TOPIC_CREATION")
}

// PEERDB_INTERVAL_SINCE_LAST_NORMALIZE_THRESHOLD_MINUTES, 0 disables normalize gap alerting entirely
func PeerDBIntervalSinceLastNormalizeThresholdMinutes(ctx context.Context, env map[string]string) (uint32, error) {
	return dynamicConfUnsigned[uint32](ctx, env, "PEERDB_INTERVAL_SINCE_LAST_NORMALIZE_THRESHOLD_MINUTES")
}

func PeerDBApplicationNamePerMirrorName(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_APPLICATION_NAME_PER_MIRROR_NAME")
}

func PeerDBMaintenanceModeEnabled(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_MAINTENANCE_MODE_ENABLED")
}

func UpdatePeerDBMaintenanceModeEnabled(ctx context.Context, pool shared.CatalogPool, enabled bool) error {
	return UpdateDynamicSetting(ctx, pool, "PEERDB_MAINTENANCE_MODE_ENABLED", ptr.String(strconv.FormatBool(enabled)))
}

func PeerDBPKMEmptyBatchThrottleThresholdSeconds(ctx context.Context, env map[string]string) (int64, error) {
	return dynamicConfSigned[int64](ctx, env, "PEERDB_PKM_EMPTY_BATCH_THROTTLE_THRESHOLD_SECONDS")
}

func PeerDBClickHouseNormalizationParts(ctx context.Context, env map[string]string) (uint64, error) {
	return dynamicConfUnsigned[uint64](ctx, env, "PEERDB_CLICKHOUSE_NORMALIZATION_PARTS")
}

func PeerDBClickHouseInitialLoadPartsPerPartition(ctx context.Context, env map[string]string) (uint64, error) {
	return dynamicConfUnsigned[uint64](ctx, env, "PEERDB_CLICKHOUSE_INITIAL_LOAD_PARTS_PER_PARTITION")
}

func PeerDBSkipSnapshotExport(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_SKIP_SNAPSHOT_EXPORT")
}

func PeerDBSourceSchemaAsDestinationColumn(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN")
}

func PeerDBPostgresCDCHandleInheritanceForNonPartitionedTables(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_POSTGRES_CDC_HANDLE_INHERITANCE_FOR_NON_PARTITIONED_TABLES")
}
