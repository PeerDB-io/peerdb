package dynamicconf

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/exp/constraints"

	utils "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/logger"
)

func dynamicConfKeyExists(ctx context.Context, conn *pgxpool.Pool, key string) bool {
	var exists pgtype.Bool
	query := "SELECT EXISTS(SELECT 1 FROM dynamic_settings WHERE config_name = $1)"
	err := conn.QueryRow(ctx, query, key).Scan(&exists)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to check if key exists: %v", err)
		return false
	}

	return exists.Bool
}

func dynamicConfSigned[T constraints.Signed](ctx context.Context, key string, defaultValue T) T {
	conn, err := utils.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get catalog connection pool: %v", err)
		return defaultValue
	}

	if !dynamicConfKeyExists(ctx, conn, key) {
		return defaultValue
	}

	var value pgtype.Text
	query := "SELECT config_value FROM alerting_settings WHERE config_name = $1"
	err = conn.QueryRow(ctx, query, key).Scan(&value)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get key: %v", err)
		return defaultValue
	}

	result, err := strconv.ParseInt(value.String, 10, 32)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse uint32: %v", err)
		return defaultValue
	}

	return T(result)
}

func dynamicConfUnsigned[T constraints.Unsigned](ctx context.Context, key string, defaultValue T) T {
	conn, err := utils.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get catalog connection pool: %v", err)
		return defaultValue
	}

	if !dynamicConfKeyExists(ctx, conn, key) {
		return defaultValue
	}

	var value pgtype.Text
	query := "SELECT config_value FROM alerting_settings WHERE config_name = $1"
	err = conn.QueryRow(ctx, query, key).Scan(&value)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get key: %v", err)
		return defaultValue
	}

	result, err := strconv.ParseUint(value.String, 10, 32)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse uint32: %v", err)
		return defaultValue
	}

	return T(result)
}

// PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD, 0 disables slot lag alerting entirely
func PeerDBSlotLagMBAlertThreshold(ctx context.Context) uint32 {
	return dynamicConfUnsigned[uint32](ctx, "PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD", 5000)
}

// PEERDB_ALERTING_GAP_MINUTES, 0 disables all alerting entirely
func PeerDBAlertingGapMinutesAsDuration(ctx context.Context) time.Duration {
	why := dynamicConfUnsigned[uint32](ctx, "PEERDB_ALERTING_GAP_MINUTES", 15)
	return time.Duration(why) * time.Minute
}

// PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD, 0 disables open connections alerting entirely
func PeerDBOpenConnectionsAlertThreshold(ctx context.Context) uint32 {
	return dynamicConfUnsigned[uint32](ctx, "PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD", 5)
}

func PeerDBSnowflakeMergeParallelism(ctx context.Context) int {
	return dynamicConfSigned(ctx, "PEERDB_SNOWFLAKE_MERGE_PARALLELISM", 8)
}

// PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD
func PeerDBCDCDiskSpillRecordsThreshold(ctx context.Context) int64 {
	return dynamicConfSigned[int64](ctx, "PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD", 1_000_000)
}

// PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD, negative numbers means memory threshold disabled
func PeerDBCDCDiskSpillMemPercentThreshold(ctx context.Context) int64 {
	return dynamicConfSigned[int64](ctx, "PEERDB_CDC_DISK_SPILL_MEM_PERCENT_THRESHOLD", -1)
}

// PEERDB_CDC_CHANNEL_BUFFER_SIZE
func PeerDBCDCChannelBufferSize(ctx context.Context) int {
	return dynamicConfSigned(ctx, "PEERDB_CDC_CHANNEL_BUFFER_SIZE", 1<<18)
}

// PEERDB_EVENTHUB_FLUSH_TIMEOUT_SECONDS
func PeerDBEventhubFlushTimeoutSeconds(ctx context.Context) time.Duration {
	x := dynamicConfSigned[int64](ctx, "PEERDB_EVENTHUB_FLUSH_TIMEOUT_SECONDS", 10)
	return time.Duration(x) * time.Second
}

// GOMEMLIMIT is a variable internal to Golang itself, we use this for internal targets, 0 means no maximum
func PeerDBFlowWorkerMaxMemBytes(ctx context.Context) uint32 {
	return dynamicConfUnsigned[uint32](ctx, "GOMEMLIMIT", 0)
}
