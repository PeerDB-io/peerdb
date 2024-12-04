package peerdbenv

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/exp/constraints"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
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
		DefaultValue:     "false",
		ValueType:        protos.DynconfValueType_BOOL,
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
		DefaultValue:     "0",
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
}

var DynamicIndex = func() map[string]int {
	defaults := make(map[string]int, len(DynamicSettings))
	for i, setting := range DynamicSettings {
		defaults[setting.Name] = i
	}
	return defaults
}()

func dynLookup(ctx context.Context, env map[string]string, key string) (string, error) {
	if val, ok := env[key]; ok {
		return val, nil
	}

	conn, err := GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		shared.LoggerFromCtx(ctx).Error("Failed to get catalog connection pool", slog.Any("error", err))
		return "", fmt.Errorf("failed to get catalog connection pool: %w", err)
	}

	var value pgtype.Text
	query := "SELECT config_value FROM dynamic_settings WHERE config_name=$1"
	if err := conn.QueryRow(ctx, query, key).Scan(&value); err != nil && err != pgx.ErrNoRows {
		shared.LoggerFromCtx(ctx).Error("Failed to get key", slog.Any("error", err))
		return "", fmt.Errorf("failed to get key: %w", err)
	}
	if !value.Valid {
		if val, ok := os.LookupEnv(key); ok {
			return val, nil
		}
		if idx, ok := DynamicIndex[key]; ok {
			return DynamicSettings[idx].DefaultValue, nil
		}
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
		shared.LoggerFromCtx(ctx).Error("Failed to parse as int64", slog.String("key", key), slog.Any("error", err))
		return 0, fmt.Errorf("failed to parse %s as int64: %w", key, err)
	}

	return T(value), nil
}

func dynamicConfUnsigned[T constraints.Unsigned](ctx context.Context, env map[string]string, key string) (T, error) {
	value, err := dynLookupConvert(ctx, env, key, func(value string) (uint64, error) {
		return strconv.ParseUint(value, 10, 64)
	})
	if err != nil {
		shared.LoggerFromCtx(ctx).Error("Failed to parse as uint64", slog.String("key", key), slog.Any("error", err))
		return 0, fmt.Errorf("failed to parse %s as uint64: %w", key, err)
	}

	return T(value), nil
}

func dynamicConfBool(ctx context.Context, env map[string]string, key string) (bool, error) {
	value, err := dynLookupConvert(ctx, env, key, strconv.ParseBool)
	if err != nil {
		shared.LoggerFromCtx(ctx).Error("Failed to parse bool", slog.String("key", key), slog.Any("error", err))
		return false, fmt.Errorf("failed to parse %s as bool: %w", key, err)
	}

	return value, nil
}

func UpdateDynamicSetting(ctx context.Context, pool *pgxpool.Pool, name string, value *string) error {
	if pool == nil {
		var err error
		pool, err = GetCatalogConnectionPoolFromEnv(ctx)
		if err != nil {
			shared.LoggerFromCtx(ctx).Error("Failed to get catalog connection pool for dynamic setting update", slog.Any("error", err))
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

func PeerDBCDCChannelBufferSize(ctx context.Context, env map[string]string) (int64, error) {
	return dynamicConfSigned[int64](ctx, env, "PEERDB_CDC_CHANNEL_BUFFER_SIZE")
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

func PeerDBFullRefreshOverwriteMode(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_FULL_REFRESH_OVERWRITE_MODE")
}

func PeerDBNullable(ctx context.Context, env map[string]string) (bool, error) {
	return dynamicConfBool(ctx, env, "PEERDB_NULLABLE")
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

func UpdatePeerDBMaintenanceModeEnabled(ctx context.Context, pool *pgxpool.Pool, enabled bool) error {
	return UpdateDynamicSetting(ctx, pool, "PEERDB_MAINTENANCE_MODE_ENABLED", ptr.String(strconv.FormatBool(enabled)))
}
