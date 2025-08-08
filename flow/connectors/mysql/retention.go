package connmysql

import (
	"context"
	"errors"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *MySqlConnector) GetLogRetentionHours(ctx context.Context) (float64, error) {
	switch c.config.Flavor {
	case protos.MySqlFlavor_MYSQL_MARIA:
		return c.getLogRetentionHoursForMariaDB(ctx)
	case protos.MySqlFlavor_MYSQL_MYSQL:
		cmp, err := c.CompareServerVersion(ctx, "8.0.1")
		if err != nil {
			return 0, fmt.Errorf("failed to get server version: %w", err)
		}
		if cmp < 0 {
			return c.getLogRetentionHoursForMySQL5(ctx)
		}
		return c.getLogRetentionHoursForMySQL8(ctx)
	default:
		return 0, fmt.Errorf("unsupported MySQL flavor: %s", c.config.Flavor)
	}
}

func (c *MySqlConnector) getLogRetentionHoursForMySQL5(ctx context.Context) (float64, error) {
	// expire_logs_days
	rs, err := c.Execute(ctx, "SELECT @@expire_logs_days")
	if err != nil {
		return 0, fmt.Errorf("failed to get expire_logs_days: %w", err)
	}
	if len(rs.Values) == 0 || len(rs.Values[0]) == 0 {
		return 0, errors.New("no value returned for expire_logs_days")
	}
	expireLogsDays := rs.Values[0][0].AsUint64()
	return float64(expireLogsDays) * 24.0, nil // convert days to hours
}

func (c *MySqlConnector) getLogRetentionHoursForMySQL8(ctx context.Context) (float64, error) {
	rs, err := c.Execute(ctx, "SELECT @@binlog_expire_logs_seconds")
	if err != nil {
		return 0, fmt.Errorf("failed to get binlog_expire_logs_seconds: %w", err)
	}
	if len(rs.Values) == 0 || len(rs.Values[0]) == 0 {
		return 0, errors.New("no value returned for binlog_expire_logs_seconds")
	}
	binlogExpireLogsSeconds := rs.Values[0][0].AsUint64()
	if binlogExpireLogsSeconds == 0 {
		return 0, nil // no expiration set
	}
	return float64(binlogExpireLogsSeconds) / 3600.0, nil
}

func (c *MySqlConnector) getLogRetentionHoursForMariaDB(ctx context.Context) (float64, error) {
	cmp, err := c.CompareServerVersion(ctx, "10.6.1")
	if err != nil {
		return 0, fmt.Errorf("failed to get server version: %w", err)
	}
	if cmp < 0 {
		return 0, fmt.Errorf("mariadb version does not support binlog_expire_logs_seconds")
	}

	rs, err := c.Execute(ctx, "SELECT @@binlog_expire_logs_seconds")
	if err != nil {
		return 0, fmt.Errorf("failed to get binlog_expire_logs_seconds: %w", err)
	}
	if len(rs.Values) == 0 || len(rs.Values[0]) == 0 {
		return 0, errors.New("no value returned for binlog_expire_logs_seconds")
	}
	binlogExpireLogsSeconds := rs.Values[0][0].AsUint64()
	return float64(binlogExpireLogsSeconds) / 3600.0, nil
}
