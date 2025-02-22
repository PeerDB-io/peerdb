package connmysql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *MySqlConnector) CheckSourceTables(ctx context.Context, tableNames []*utils.SchemaTable) error {
	for _, parsedTable := range tableNames {
		query := fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1", parsedTable.Schema, parsedTable.Table)
		_, err := c.Execute(ctx, query)
		if err != nil {
			return fmt.Errorf("error checking table %s.%s: %w", parsedTable.Schema, parsedTable.Table, err)
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
		if strings.Contains(grant, "REPLICATION SLAVE") || strings.Contains(grant, "REPLICATION CLIENT") {
			return nil
		}
	}

	return errors.New("MySQL user does not have replication privileges")
}

func (c *MySqlConnector) CheckReplicationConnectivity(ctx context.Context) error {
	rs, err := c.Execute(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return fmt.Errorf("failed to check replication status: %w", err)
	}
	if rs.RowNumber() == 0 {
		return errors.New("binary logging is disabled on this MySQL server")
	}

	masterLogFile := rs.Values[0][0].AsString()
	masterLogPos := rs.Values[0][1].AsInt64()

	// Additional validation: Check if the values are valid
	if len(masterLogFile) == 0 || masterLogPos <= 0 {
		return errors.New("invalid replication status: missing log file or position")
	}

	return nil
}

func (c *MySqlConnector) CheckBinlogSettings(ctx context.Context) error {
	// Check binlog_expire_logs_seconds
	rs, err := c.Execute(ctx, "SELECT @@binlog_expire_logs_seconds")
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_expire_logs_seconds: %w", err)
	}

	if len(rs.Values) == 0 || len(rs.Values[0]) == 0 {
		return errors.New("no value returned for binlog_expire_logs_seconds")
	}
	// Convert FieldValue to int
	expireSecondsStr := shared.UnsafeFastReadOnlyBytesToString(rs.Values[0][0].AsString())
	expireSeconds, err := strconv.Atoi(expireSecondsStr)
	if err != nil {
		return fmt.Errorf("failed to parse binlog_expire_logs_seconds: %w", err)
	}

	if expireSeconds <= 86400 {
		return errors.New("binlog_expire_logs_seconds is too low. Must be greater than 1 day")
	}

	// Check binlog_format
	rs, err = c.Execute(ctx, "SELECT @@binlog_format")
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_format: %w", err)
	}

	if len(rs.Values) == 0 || len(rs.Values[0]) == 0 {
		return errors.New("no value returned for binlog_format")
	}

	// Convert FieldValue to string safely
	binlogFormat := shared.UnsafeFastReadOnlyBytesToString(rs.Values[0][0].AsString())
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW'")
	}

	// Check binlog_row_value_options
	rs, err = c.Execute(ctx, "SELECT @@binlog_row_value_options")
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_row_value_options: %w", err)
	}

	if len(rs.Values) == 0 || len(rs.Values[0]) == 0 {
		return errors.New("no value returned for binlog_row_value_options")
	}

	// Convert FieldValue to string safely
	binlogRowValueOptions := shared.UnsafeFastReadOnlyBytesToString(rs.Values[0][0].AsString())
	if binlogRowValueOptions != "" {
		return errors.New("binlog_row_value_options must be disabled to prevent JSON change deltas")
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

	if err := c.CheckBinlogSettings(ctx); err != nil {
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
			return err
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

	if c.config.Flavor == protos.MySqlFlavor_MYSQL_MYSQL {
		if err := c.CheckBinlogSettings(ctx); err != nil {
			return fmt.Errorf("binlog configuration error: %w", err)
		}
	}

	return nil
}
