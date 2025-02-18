package connmysql


import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type MySQLConnector struct {
	conn *sql.DB
	config *MySQLConfig
}

func (c *MySQLConnector) CheckSourceTables(ctx context.Context, tableNames []*utils.SchemaTable) error {
	if c.conn == nil {
		return errors.New("check tables: conn is nil")
	}

	for _, parsedTable := range tableNames {
		query := fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1", parsedTable.Schema, parsedTable.Table)
		_, err := c.conn.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("error checking table %s.%s: %v", parsedTable.Schema, parsedTable.Table, err)
		}
	}
	return nil
}

func (c *MySQLConnector) CheckReplicationPermissions(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("check replication permissions: conn is nil")
	}

	var replicationPrivilege string
	err := c.conn.QueryRowContext(ctx, "SHOW GRANTS FOR CURRENT_USER()").Scan(&replicationPrivilege)
	if err != nil {
		return fmt.Errorf("failed to check replication privileges: %v", err)
	}

	if !strings.Contains(replicationPrivilege, "REPLICATION SLAVE") && !strings.Contains(replicationPrivilege, "REPLICATION CLIENT") {
		return errors.New("MySQL user does not have replication privileges")
	}

	return nil
}

func (c *MySQLConnector) CheckReplicationConnectivity(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("check replication connectivity: conn is nil")
	}

	var masterLogFile string
	var masterLogPos int

	err := c.conn.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&masterLogFile, &masterLogPos)
	if err != nil {
		// Handle case where SHOW MASTER STATUS returns no rows (binary logging disabled)
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("binary logging is disabled on this MySQL server")
		}
		return fmt.Errorf("failed to check replication status: %v", err)
	}

	// Additional validation: Check if the values are valid
	if masterLogFile == "" || masterLogPos <= 0 {
		return errors.New("invalid replication status: missing log file or position")
	}

	return nil
}

func (c *MySQLConnector) CheckBinlogSettings(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("check binlog settings: conn is nil")
	}

	// Check binlog_expire_logs_seconds
	var expireSeconds int
	err := c.conn.QueryRowContext(ctx, "SELECT @@binlog_expire_logs_seconds").Scan(&expireSeconds)
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_expire_logs_seconds: %v", err)
	}
	if expireSeconds <= 86400 {
		return errors.New("binlog_expire_logs_seconds is too low. Must be greater than 1 day")
	}

	// Check binlog_format
	var binlogFormat string
	err = c.conn.QueryRowContext(ctx, "SELECT @@binlog_format").Scan(&binlogFormat)
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_format: %v", err)
	}
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW'")
	}

	// Check binlog_row_metadata
	var binlogRowMetadata string
	err = c.conn.QueryRowContext(ctx, "SELECT @@binlog_row_metadata").Scan(&binlogRowMetadata)
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_row_metadata: %v", err)
	}
	if binlogRowMetadata != "FULL" {
		return errors.New("binlog_row_metadata must be set to 'FULL' for column exclusion support")
	}

	// Check binlog_row_image
	var binlogRowImage string
	err = c.conn.QueryRowContext(ctx, "SELECT @@binlog_row_image").Scan(&binlogRowImage)
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_row_image: %v", err)
	}
	if binlogRowImage != "FULL" {
		return errors.New("binlog_row_image must be set to 'FULL' (equivalent to PostgreSQL's REPLICA IDENTITY FULL)")
	}

	// Check binlog_row_value_options
	var binlogRowValueOptions string
	err = c.conn.QueryRowContext(ctx, "SELECT @@binlog_row_value_options").Scan(&binlogRowValueOptions)
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_row_value_options: %v", err)
	}
	if binlogRowValueOptions != "" {
		return errors.New("binlog_row_value_options must be disabled to prevent JSON change deltas")
	}

	return nil
}

func (c *MySQLConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	sourceTables := make([]*utils.SchemaTable, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		parsedTable, parseErr := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			return fmt.Errorf("invalid source table identifier: %s", parseErr)
		}
		sourceTables = append(sourceTables, parsedTable)
	}

	if err := c.CheckReplicationConnectivity(ctx); err != nil {
		return fmt.Errorf("unable to establish replication connectivity: %v", err)
	}

	if err := c.CheckReplicationPermissions(ctx); err != nil {
		return fmt.Errorf("failed to check replication permissions: %v", err)
	}

	if err := c.CheckSourceTables(ctx, sourceTables); err != nil {
		return fmt.Errorf("provided source tables invalidated: %v", err)
	}

	if err := c.CheckBinlogSettings(ctx); err != nil {
		return fmt.Errorf("binlog configuration error: %v", err)
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
		return fmt.Errorf("unable to establish replication connectivity: %v", err)
	}

	if err := c.CheckReplicationPermissions(ctx); err != nil {
		return fmt.Errorf("failed to check replication permissions: %v", err)
	}

	if err := c.CheckBinlogSettings(ctx); err != nil {
		return fmt.Errorf("binlog configuration error: %v", err)
	}

	return nil
}

func (c *MySqlConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	return nil
}
