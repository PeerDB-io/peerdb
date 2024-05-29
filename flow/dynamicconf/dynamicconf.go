package dynamicconf

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/constraints"

	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

func dynamicConfSigned[T constraints.Signed](ctx context.Context, key string) (T, error) {
	conn, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get catalog connection pool: %v", err)
		return 0, fmt.Errorf("failed to get catalog connection pool: %w", err)
	}

	var value pgtype.Text
	query := "SELECT coalesce(config_value,config_default_value) FROM dynamic_settings WHERE config_name=$1"
	err = conn.QueryRow(ctx, query, key).Scan(&value)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get key: %v", err)
		return 0, fmt.Errorf("failed to get key: %w", err)
	}

	result, err := strconv.ParseInt(value.String, 10, 64)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse as int64: %v", err)
		return 0, fmt.Errorf("failed to parse as int64: %w", err)
	}

	return T(result), nil
}

func dynamicConfUnsigned[T constraints.Unsigned](ctx context.Context, key string) (T, error) {
	conn, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get catalog connection pool: %v", err)
		return 0, fmt.Errorf("failed to get catalog connection pool: %w", err)
	}

	var value pgtype.Text
	query := "SELECT coalesce(config_value,config_default_value) FROM dynamic_settings WHERE config_name=$1"
	err = conn.QueryRow(ctx, query, key).Scan(&value)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get key: %v", err)
		return 0, fmt.Errorf("failed to get key: %w", err)
	}

	result, err := strconv.ParseUint(value.String, 10, 64)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse as int64: %v", err)
		return 0, fmt.Errorf("failed to parse as int64: %w", err)
	}

	return T(result), nil
}

func dynamicConfBool(ctx context.Context, key string) (bool, error) {
	conn, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get catalog connection pool: %v", err)
		return false, fmt.Errorf("failed to get catalog connection pool: %w", err)
	}

	var value pgtype.Text
	query := "SELECT coalesce(config_value,config_default_value) FROM dynamic_settings WHERE config_name = $1"
	err = conn.QueryRow(ctx, query, key).Scan(&value)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to get key: %v", err)
		return false, fmt.Errorf("failed to get key: %w", err)
	}

	result, err := strconv.ParseBool(value.String)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("Failed to parse bool: %v", err)
		return false, fmt.Errorf("failed to parse bool: %w", err)
	}

	return result, nil
}

// PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD, 0 disables slot lag alerting entirely
func PeerDBSlotLagMBAlertThreshold(ctx context.Context) (uint32, error) {
	return dynamicConfUnsigned[uint32](ctx, "PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD")
}

// PEERDB_ALERTING_GAP_MINUTES, 0 disables all alerting entirely
func PeerDBAlertingGapMinutesAsDuration(ctx context.Context) (time.Duration, error) {
	why, err := dynamicConfUnsigned[uint32](ctx, "PEERDB_ALERTING_GAP_MINUTES")
	if err != nil {
		return 0, err
	}
	return time.Duration(int64(why)) * time.Minute, nil
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
