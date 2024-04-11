package dynamicconf

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

func dynamicConfKeyExists(ctx context.Context, conn *pgxpool.Pool, key string) bool {
	var exists pgtype.Bool
	query := "SELECT EXISTS(SELECT 1 FROM alerting_settings WHERE config_name = $1)"
	err := conn.QueryRow(ctx, query, key).Scan(&exists)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to check if key exists: %v", err)
		return false
	}

	return exists.Bool
}

func dynamicConfUint32(ctx context.Context, key string, defaultValue uint32) uint32 {
	conn, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
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

	return uint32(result)
}

func dynamicConfBool(ctx context.Context, key string, defaultValue bool) bool {
	conn, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
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

	result, err := strconv.ParseBool(value.String)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse bool: %v", err)
		return defaultValue
	}

	return result
}

// PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD, 0 disables slot lag alerting entirely
func PeerDBSlotLagMBAlertThreshold(ctx context.Context) uint32 {
	return dynamicConfUint32(ctx, "PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD", 5000)
}

// PEERDB_ALERTING_GAP_MINUTES, 0 disables all alerting entirely
func PeerDBAlertingGapMinutesAsDuration(ctx context.Context) time.Duration {
	why := int64(dynamicConfUint32(ctx, "PEERDB_ALERTING_GAP_MINUTES", 15))
	return time.Duration(why) * time.Minute
}

// PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD, 0 disables open connections alerting entirely
func PeerDBOpenConnectionsAlertThreshold(ctx context.Context) uint32 {
	return dynamicConfUint32(ctx, "PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD", 5)
}

// PEERDB_BIGQUERY_ENABLE_SYNCED_AT_PARTITIONING_BY_DAYS, for creating target tables with
// partitioning by _PEERDB_SYNCED_AT column
// If true, the target tables will be partitioned by _PEERDB_SYNCED_AT column
// If false, the target tables will not be partitioned
func PeerDBBigQueryEnableSyncedAtPartitioning(ctx context.Context) bool {
	return dynamicConfBool(ctx, "PEERDB_BIGQUERY_ENABLE_SYNCED_AT_PARTITIONING_BY_DAYS", false)
}
