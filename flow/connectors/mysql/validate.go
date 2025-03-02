package connmysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *MySqlConnector) CheckSourceTables(ctx context.Context, tableNames []*utils.SchemaTable) error {
	for _, parsedTable := range tableNames {
		if _, err := c.Execute(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", parsedTable.MySQL())); err != nil {
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
		// Normalize and split grant string into individual permissions
		grantParts := strings.FieldsFunc(grant, func(r rune) bool { return r == ' ' || r == ',' })

		// Use a map for efficient lookup
		grantSet := make(map[string]struct{}, len(grantParts))
		for _, part := range grantParts {
			grantSet[part] = struct{}{}
		}

		// Check for exact match of required privileges
		if _, ok := grantSet["REPLICATION SLAVE"]; ok {
			return nil
		}
		if _, ok := grantSet["REPLICATION CLIENT"]; ok {
			return nil
		}
	}

	return errors.New("MySQL user does not have replication privileges")
}

func (c *MySqlConnector) CheckReplicationConnectivity(ctx context.Context) error {
	namePos, err := c.GetMasterPos(ctx)
	if err != nil {
		return fmt.Errorf("failed to check replication status: %w", err)
	}

	masterLogFile, masterLogPos := namePos.Name, namePos.Pos

	// Additional validation: Check if the values are valid
	if masterLogFile == "" || masterLogPos <= 0 {
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

	expireSeconds := rs.Values[0][0].AsUint64()

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

	binlogRowValueOptions := shared.UnsafeFastReadOnlyBytesToString(rs.Values[0][0].AsString())
	if binlogRowValueOptions != "" {
		return errors.New("binlog_row_value_options must be disabled to prevent JSON change deltas")
	}

	// Check binlog_row_metadata
	rs, err = c.Execute(ctx, "SELECT @@binlog_row_metadata")
	if err != nil {
		c.logger.Warn("failed to retrieve binlog_row_metadata", "error", err)
		return nil
	}

	if len(rs.Values) == 0 {
		c.logger.Warn("failed to retrieve binlog_row_metadata: no rows returned")
		return nil
	}

	binlogRowMetadata := shared.UnsafeFastReadOnlyBytesToString(rs.Values[0][0].AsString()) // Convert FieldValue to string

	if binlogRowMetadata != "FULL" {
		c.logger.Warn("binlog_row_metadata must be set to 'FULL' for column exclusion support")
		return nil
	}

	// Check binlog_row_image
	rs, err = c.Execute(ctx, "SELECT @@binlog_row_image")
	if err != nil {
		c.logger.Warn("failed to retrieve binlog_row_image: ", "error", err)
		return nil
	}

	if len(rs.Values) == 0 {
		c.logger.Warn("failed to retrieve binlog_row_image: no rows returned")
		return nil
	}

	binlogRowImage := shared.UnsafeFastReadOnlyBytesToString(rs.Values[0][0].AsString()) // Convert FieldValue to string

	if binlogRowImage != "FULL" {
		c.logger.Warn("binlog_row_image must be set to 'FULL' (equivalent to PostgreSQL's REPLICA IDENTITY FULL)")
		return nil
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
