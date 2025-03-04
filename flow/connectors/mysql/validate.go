package connmysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

func (c *MySqlConnector) CheckReplicationPermissions(ctx context.Context) error {
	rs, err := c.Execute(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return fmt.Errorf("failed to check replication privileges: %w", err)
	}

	for _, row := range rs.Values {
		grant := shared.UnsafeFastReadOnlyBytesToString(row[0].AsString())
		for permission := range strings.FieldsFuncSeq(grant, func(r rune) bool { return r == ',' }) {
			permission = strings.TrimSpace(permission)
			if permission == "REPLICATION SLAVE" || permission == "REPLICATION CLIENT" {
				return nil
			}
		}
	}

	return errors.New("MySQL user does not have replication privileges")
}

func (c *MySqlConnector) CheckReplicationConnectivity(ctx context.Context) error {
	namePos, err := c.GetMasterPos(ctx)
	if err != nil {
		return fmt.Errorf("failed to check replication status: %w", err)
	}

	if namePos.Name == "" || namePos.Pos <= 0 {
		return errors.New("invalid replication status: missing log file or position")
	}

	return nil
}

func (c *MySqlConnector) CheckBinlogSettings(ctx context.Context, requireRowMetadata bool) error {
	if c.config.Flavor == protos.MySqlFlavor_MYSQL_MYSQL {
		return nil
	}

	cmp, err := c.CompareServerVersion(ctx, "8.0.1")
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	if cmp < 0 {
		c.logger.Warn("cannot validate mysql prior to 8.0.1")
		return nil
	}
	query := "SELECT @@binlog_expire_logs_seconds, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata"

	checkRowValueOptions := false
	cmp, err = c.CompareServerVersion(ctx, "8.0.3")
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	if cmp >= 0 {
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
	if binlogExpireLogsSeconds < 86400 {
		c.logger.Warn("binlog_expire_logs_seconds should be at least 24 hours",
			slog.Uint64("binlog_expire_logs_seconds", binlogExpireLogsSeconds))
	}

	binlogFormat := shared.UnsafeFastReadOnlyBytesToString(row[1].AsString())
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW', currently " + binlogFormat)
	}

	if checkRowValueOptions {
		binlogRowValueOptions := shared.UnsafeFastReadOnlyBytesToString(row[4].AsString())
		if binlogRowValueOptions != "" {
			return errors.New("binlog_row_value_options must be disabled, currently " + binlogRowValueOptions)
		}
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

	binlogRowImage := shared.UnsafeFastReadOnlyBytesToString(row[2].AsString())
	if binlogRowImage != "FULL" {
		c.logger.Warn("binlog_row_image should be set to 'FULL' to avoid missing data",
			slog.String("binlog_row_image", strings.Clone(binlogRowImage)))
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

	if err := c.CheckReplicationPermissions(ctx); err != nil {
		return fmt.Errorf("failed to check replication permissions: %w", err)
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
	if _, err := c.Execute(ctx, "select @@gtid_mode"); err != nil {
		var mErr *mysql.MyError
		// seems to be MariaDB
		if errors.As(err, &mErr) && mErr.Code == mysql.ER_UNKNOWN_SYSTEM_VARIABLE && c.config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
			return errors.New("server appears to be MariaDB but flavor is not set to MariaDB")
		} else {
			return nil
		}
	}
	if c.config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		return errors.New("flavor is set to MariaDB but the server appears to be MySQL")
	} else if c.config.Flavor == protos.MySqlFlavor_MYSQL_UNKNOWN {
		return errors.New("flavor is set to unknown")
	}

	if err := c.CheckReplicationConnectivity(ctx); err != nil {
		return fmt.Errorf("unable to establish replication connectivity: %w", err)
	}

	if err := c.CheckReplicationPermissions(ctx); err != nil {
		return fmt.Errorf("failed to check replication permissions: %w", err)
	}

	if err := c.CheckBinlogSettings(ctx, false); err != nil {
		return fmt.Errorf("binlog configuration error: %w", err)
	}

	return nil
}
