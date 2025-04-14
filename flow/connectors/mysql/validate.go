package connmysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *MySqlConnector) CheckSourceTables(ctx context.Context, tableNames []*utils.SchemaTable) error {
	for _, parsedTable := range tableNames {
		if _, err := c.Execute(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", parsedTable.MySQL())); err != nil {
			return fmt.Errorf("error checking table %s: %w", parsedTable.MySQL(), err)
		}
	}
	return nil
}

func (c *MySqlConnector) CheckReplicationConnectivity(ctx context.Context) error {
	if c.config.ReplicationMechanism == protos.MySqlReplicationMechanism_MYSQL_GTID {
		if _, err := c.GetMasterGTIDSet(ctx); err != nil {
			return fmt.Errorf("failed to check replication status: %w", err)
		}
	} else {
		namePos, err := c.GetMasterPos(ctx)
		if err != nil {
			return fmt.Errorf("failed to check replication status: %w", err)
		}

		if namePos.Name == "" || namePos.Pos <= 0 {
			return errors.New("invalid replication status: missing log file or position")
		}
	}
	return nil
}

// only for MySQL 5.7 and below
func (c *MySqlConnector) checkMySQL5_BinlogSettings(ctx context.Context) error {
	cmp, err := c.CompareServerVersion(ctx, "5.5.0")
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	if cmp < 0 {
		c.logger.Warn("cannot validate mysql prior to 5.5.0, uncharted territory")
		return nil
	}

	// MySQL 5.6.6 introduced GTIDs so they may work, no need to enforce filepos for now

	query := "SELECT @@binlog_format"
	checkBinlogRowImage := false
	cmp, err = c.CompareServerVersion(ctx, "5.6.2")
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	if cmp >= 0 {
		query += ", @@binlog_row_image"
		checkBinlogRowImage = true
	}

	// binlog_expire_logs_seconds was introduced in 8.0 https://dev.mysql.com/worklog/task/?id=10924
	// since expire_logs_days has day granularity, all settings of it work for us so not checking
	rs, err := c.Execute(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to retrieve settings <5.7: %w", err)
	}
	if len(rs.Values) == 0 {
		return errors.New("no value returned for settings <5.7")
	}
	row := rs.Values[0]

	binlogFormat := shared.UnsafeFastReadOnlyBytesToString(row[0].AsString())
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW', currently " + binlogFormat)
	}
	if checkBinlogRowImage {
		binlogRowImage := shared.UnsafeFastReadOnlyBytesToString(row[1].AsString())
		if binlogRowImage != "FULL" {
			return errors.New("binlog_row_image must be set to 'FULL', currently " + binlogRowImage)
		}
	}

	return nil
}

func (c *MySqlConnector) CheckBinlogSettings(ctx context.Context, requireRowMetadata bool) error {
	if c.config.Flavor == protos.MySqlFlavor_MYSQL_MYSQL {
		cmp, err := c.CompareServerVersion(ctx, "8.0.1")
		if err != nil {
			return fmt.Errorf("failed to get server version: %w", err)
		}
		if cmp < 0 {
			if requireRowMetadata {
				return errors.New(
					"MySQL version too old for column exclusion support, " +
						"please disable it or upgrade to >8.0.1 (binlog_row_metadata needed)",
				)
			}
			c.logger.Warn("cannot validate mysql prior to 8.0.1, falling back to MySQL 5.7 check")
			return c.checkMySQL5_BinlogSettings(ctx)
		}
	}

	query := "SELECT @@binlog_expire_logs_seconds, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata"

	checkRowValueOptions := false
	cmp, err := c.CompareServerVersion(ctx, "8.0.3")
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	// Don't see this setting on any MariaDB version
	if cmp >= 0 && c.config.Flavor == protos.MySqlFlavor_MYSQL_MYSQL {
		checkRowValueOptions = true
		query += ", @@binlog_row_value_options"
	}

	rs, err := c.Execute(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to retrieve settings: %w", err)
	}
	if len(rs.Values) == 0 {
		return errors.New("no value returned for settings")
	}
	row := rs.Values[0]

	binlogExpireLogsSeconds := row[0].AsUint64()
	if binlogExpireLogsSeconds < 86400 && binlogExpireLogsSeconds != 0 {
		c.logger.Warn("binlog_expire_logs_seconds should be at least 24 hours",
			slog.Uint64("binlog_expire_logs_seconds", binlogExpireLogsSeconds))
	}

	binlogFormat := shared.UnsafeFastReadOnlyBytesToString(row[1].AsString())
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW', currently " + binlogFormat)
	}

	binlogRowImage := shared.UnsafeFastReadOnlyBytesToString(row[2].AsString())
	if binlogRowImage != "FULL" {
		c.logger.Warn("binlog_row_image should be set to 'FULL' to avoid missing data",
			slog.String("binlog_row_image", strings.Clone(binlogRowImage)))
	}

	binlogRowMetadata := shared.UnsafeFastReadOnlyBytesToString(row[3].AsString())
	if binlogRowMetadata != "FULL" {
		if requireRowMetadata {
			return errors.New("binlog_row_metadata must be set to 'FULL' for column exclusion support, currently " + binlogRowMetadata)
		} else {
			c.logger.Warn("binlog_row_metadata should be set to 'FULL' for more reliable replication",
				slog.String("binlog_row_metadata", strings.Clone(binlogRowMetadata)))
		}
	}

	if checkRowValueOptions {
		binlogRowValueOptions := shared.UnsafeFastReadOnlyBytesToString(row[4].AsString())
		if binlogRowValueOptions != "" {
			return errors.New("binlog_row_value_options must be disabled, currently " + binlogRowValueOptions)
		}
	}

	// AWS RDS/Aurora has its own binlog retention setting that we need to check, minimum 24h
	// check RDS/Aurora binlog retention setting
	if rs, err := c.Execute(ctx, "SELECT value FROM mysql.rds_configuration WHERE name='binlog retention hours'"); err != nil {
		var mErr *mysql.MyError
		if errors.As(err, &mErr) && mErr.Code == mysql.ER_NO_SUCH_TABLE || mErr.Code == mysql.ER_TABLEACCESS_DENIED_ERROR {
			// Table doesn't exist, which means this is not RDS/Aurora
			slog.Warn("mysql.rds_configuration table does not exist, skipping Aurora/RDS binlog retention check", slog.Any("error", err))
			return nil
		}
		return errors.New("failed to check RDS/Aurora binlog retention hours: " + err.Error())
	} else if len(rs.Values) > 0 {
		binlogRetentionHoursStr := shared.UnsafeFastReadOnlyBytesToString(rs.Values[0][0].AsString())
		if binlogRetentionHoursStr == "" {
			return errors.New("RDS/Aurora setting 'binlog retention hours' should be at least 24, currently unset")
		}
		slog.Info("binlog retention hours", "binlogRetentionHours", binlogRetentionHoursStr)
		if binlogRetentionHours, err := strconv.Atoi(binlogRetentionHoursStr); err != nil {
			return errors.New("failed to parse RDS/Aurora setting 'binlog retention hours': " + err.Error())
		} else if binlogRetentionHours < 24 {
			return errors.New("RDS/Aurora setting 'binlog retention hours' should be at least 24, currently " + binlogRetentionHoursStr)
		}
	} else {
		slog.Warn("binlog retention hours returned nothing, skipping Aurora/RDS binlog retention check")
	}

	return nil
}

func (c *MySqlConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	sourceTables := make([]*utils.SchemaTable, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		parsedTable, parseErr := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			return fmt.Errorf("invalid source table identifier: %w", parseErr)
		}
		sourceTables = append(sourceTables, parsedTable)
	}

	if err := c.CheckReplicationConnectivity(ctx); err != nil {
		return fmt.Errorf("unable to establish replication connectivity: %w", err)
	}

	if err := c.CheckSourceTables(ctx, sourceTables); err != nil {
		return fmt.Errorf("provided source tables invalidated: %w", err)
	}

	requireRowMetadata := false
	for _, tm := range cfg.TableMappings {
		if len(tm.Exclude) > 0 {
			requireRowMetadata = true
			break
		}
	}
	if err := c.CheckBinlogSettings(ctx, requireRowMetadata); err != nil {
		return fmt.Errorf("binlog configuration error: %w", err)
	}

	return nil
}

func (c *MySqlConnector) ValidateCheck(ctx context.Context) error {
	if c.config.Flavor == protos.MySqlFlavor_MYSQL_UNKNOWN {
		return errors.New("flavor is set to unknown")
	}

	// MariaDB specific setting, introduced in MariaDB 10.0.3
	if rs, err := c.Execute(ctx, "select @@gtid_strict_mode"); err != nil {
		var mErr *mysql.MyError
		// seems to be MySQL
		if errors.As(err, &mErr) && mErr.Code == mysql.ER_UNKNOWN_SYSTEM_VARIABLE {
			if c.config.Flavor != protos.MySqlFlavor_MYSQL_MYSQL {
				return errors.New("server appears to be MySQL but flavor is not set to MySQL")
			}
		} else {
			return fmt.Errorf("failed to check GTID mode: %w", err)
		}
	} else if len(rs.Values) > 0 {
		if c.config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
			return errors.New("server appears to be MariaDB but flavor is not set to MariaDB")
		}
	}

	if err := c.CheckReplicationConnectivity(ctx); err != nil {
		return fmt.Errorf("unable to establish replication connectivity: %w", err)
	}

	if err := c.CheckBinlogSettings(ctx, false); err != nil {
		return fmt.Errorf("binlog configuration error: %w", err)
	}

	return nil
}
