package peerdbenv

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/constraints"

	"github.com/PeerDB-io/peer-flow/logger"
)

func dynLookup(ctx context.Context, key string) (string, error) {
	conn, err := GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get catalog connection pool", slog.Any("error", err))
		return "", fmt.Errorf("failed to get catalog connection pool: %w", err)
	}

	var value pgtype.Text
	var default_value pgtype.Text
	query := "SELECT config_value, config_default_value FROM dynamic_settings WHERE config_name=$1"
	err = conn.QueryRow(ctx, query, key).Scan(&value, &default_value)
	if err != nil {
		if err == pgx.ErrNoRows {
			if val, ok := os.LookupEnv(key); ok {
				return val, nil
			}
		}
		logger.LoggerFromCtx(ctx).Error("Failed to get key", slog.Any("error", err))
		return "", fmt.Errorf("failed to get key: %w", err)
	}
	if !value.Valid {
		if val, ok := os.LookupEnv(key); ok {
			return val, nil
		}
		return default_value.String, nil
	}
	return value.String, nil
}

func dynLookupConvert[T any](ctx context.Context, key string, fn func(string) (T, error)) (T, error) {
	value, err := dynLookup(ctx, key)
	if err != nil {
		var none T
		return none, err
	}
	return fn(value)
}

func dynamicConfSigned[T constraints.Signed](ctx context.Context, key string) (T, error) {
	value, err := dynLookupConvert(ctx, key, func(value string) (int64, error) {
		return strconv.ParseInt(value, 10, 64)
	})
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse as int64", slog.Any("error", err))
		return 0, fmt.Errorf("failed to parse as int64: %w", err)
	}

	return T(value), nil
}

func dynamicConfUnsigned[T constraints.Unsigned](ctx context.Context, key string) (T, error) {
	value, err := dynLookupConvert(ctx, key, func(value string) (uint64, error) {
		return strconv.ParseUint(value, 10, 64)
	})
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse as uint64", slog.Any("error", err))
		return 0, fmt.Errorf("failed to parse as uint64: %w", err)
	}

	return T(value), nil
}

func DynamicConfBool(ctx context.Context, key string) (bool, error) {
	return dynamicConfBool(ctx, key)

}

func dynamicConfBool(ctx context.Context, key string) (bool, error) {
	value, err := dynLookupConvert(ctx, key, strconv.ParseBool)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse bool", slog.Any("error", err))
		return false, fmt.Errorf("failed to parse bool: %w", err)
	}

	return value, nil
}

// PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD, 0 disables slot lag alerting entirely
func PeerDBSlotLagMBAlertThreshold(ctx context.Context) (uint32, error) {
	return dynamicConfUnsigned[uint32](ctx, "PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD")
}

// PEERDB_ALERTING_GAP_MINUTES, 0 disables all alerting entirely
func PeerDBAlertingGapMinutesAsDuration(ctx context.Context) (time.Duration, error) {
	why, err := dynamicConfSigned[int64](ctx, "PEERDB_ALERTING_GAP_MINUTES")
	if err != nil {
		return 0, err
	}
	return time.Duration(why) * time.Minute, nil
}

// PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD, 0 disables open connections alerting entirely
func PeerDBOpenConnectionsAlertThreshold(ctx context.Context) (uint32, error) {
	return dynamicConfUnsigned[uint32](ctx, "PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD")
}

// PEERDB_BIGQUERY_ENABLE_SYNCED_AT_PARTITIONING_BY_DAYS, for creating target tables with
// partitioning by _PEERDB_SYNCED_AT column
// If true, the target tables will be partitioned by _PEERDB_SYNCED_AT column
// If false, the target tables will not be partitioned
func PeerDBBigQueryEnableSyncedAtPartitioning(ctx context.Context) (bool, error) {
	return dynamicConfBool(ctx, "PEERDB_BIGQUERY_ENABLE_SYNCED_AT_PARTITIONING_BY_DAYS")
}

func PeerDBCDCChannelBufferSize(ctx context.Context) (int64, error) {
	return dynamicConfSigned[int64](ctx, "PEERDB_CDC_CHANNEL_BUFFER_SIZE")
}

func PeerDBQueueFlushTimeoutSeconds(ctx context.Context) (time.Duration, error) {
	x, err := dynamicConfSigned[int64](ctx, "PEERDB_QUEUE_FLUSH_TIMEOUT_SECONDS")
	if err != nil {
		return 0, err
	}
	return time.Duration(x) * time.Second, nil
}

func PeerDBQueueParallelism(ctx context.Context) (int64, error) {
	return dynamicConfSigned[int64](ctx, "PEERDB_QUEUE_PARALLELISM")
}

func PeerDBCDCDiskSpillRecordsThreshold(ctx context.Context) (int64, error) {
	return dynamicConfSigned[int64](ctx, "PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD")
}

func PeerDBCDCDiskSpillMemPercentThreshold(ctx context.Context) (int64, error) {
	return dynamicConfSigned[int64](ctx, "PEERDB_CDC_DISK_SPILL_MEM_PERCENT_THRESHOLD")
}

func PeerDBEnableWALHeartbeat(ctx context.Context) (bool, error) {
	return dynamicConfBool(ctx, "PEERDB_ENABLE_WAL_HEARTBEAT")
}

func PeerDBWALHeartbeatQuery(ctx context.Context) (string, error) {
	return dynLookup(ctx, "PEERDB_WAL_HEARTBEAT_QUERY")
}

func PeerDBEnableParallelSyncNormalize(ctx context.Context) (bool, error) {
	return dynamicConfBool(ctx, "PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE")
}

func PeerDBSnowflakeMergeParallelism(ctx context.Context) (int64, error) {
	return dynamicConfSigned[int64](ctx, "PEERDB_SNOWFLAKE_MERGE_PARALLELISM")
}

func PeerDBClickhouseAWSS3BucketName(ctx context.Context) (string, error) {
	return dynLookup(ctx, "PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME")
}

// Kafka has topic auto create as an option, auto.create.topics.enable
// But non-dedicated cluster maybe can't set config, may want peerdb to create topic. Similar for PubSub
func PeerDBQueueForceTopicCreation(ctx context.Context) (bool, error) {
	return dynamicConfBool(ctx, "PEERDB_QUEUE_FORCE_TOPIC_CREATION")
}

// experimental, don't increase to greater than 64
func PeerDBMaxSyncsPerCDCFlow(ctx context.Context) (uint32, error) {
	return dynamicConfUnsigned[uint32](ctx, "PEERDB_MAX_SYNCS_PER_CDC_FLOW")
}
