package connmysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	// "github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *MySQLConnector) CheckSourceTables(ctx context.Context, tableNames []*utils.SchemaTable) error {
	// if c.conn == nil {
	// 	return errors.New("check tables: conn is nil")
	// }

	for _, parsedTable := range tableNames {
		query := fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1", parsedTable.Schema, parsedTable.Table)
		_, err := c.conn.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("error checking table %s.%s: %w", parsedTable.Schema, parsedTable.Table, err)
		}
	}
	return nil
}

func (c *MySQLConnector) CheckReplicationPermissions(ctx context.Context) error {
	// if c.conn == nil {
	// 	return errors.New("check replication permissions: conn is nil")
	// }

	rows, err := c.Execute(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return fmt.Errorf("failed to check replication privileges: %w", err)
	}
	for _, row := range rows {
		if grant, ok := row[0].(string); ok {
			if strings.Contains(grant, "REPLICATION SLAVE") || strings.Contains(grant, "REPLICATION CLIENT") {
				return nil
			}
		}
	}

	return errors.New("MySQL user does not have replication privileges")	
}

func (c *MySQLConnector) CheckReplicationConnectivity(ctx context.Context) error {
	// if c.conn == nil {
	// 	return errors.New("check replication connectivity: conn is nil")
	// }

	rows, err := c.Execute(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return fmt.Errorf("failed to check replication status: %w", err)
	}
	if len(rows) == 0 {
		return errors.New("binary logging is disabled on this MySQL server")
	}
	
	masterLogFile, _ := rows[0][0].(string)
	masterLogPos, _ := rows[0][1].(int64)

	// Additional validation: Check if the values are valid
	if masterLogFile == "" || masterLogPos <= 0 {
		return errors.New("invalid replication status: missing log file or position")
	}	

	return nil
}

func (c *MySQLConnector) CheckBinlogSettings(ctx context.Context) error {
	// if c.conn == nil {
	// 	return errors.New("check binlog settings: conn is nil")
	// }

	rows, err := c.Execute(ctx, "SELECT @@binlog_expire_logs_seconds")
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_expire_logs_seconds: %w", err)
	}
	defer rows.Close()

	var expireSeconds int
	if rows.Next() {
		if err := rows.Scan(&expireSeconds); err != nil {
			return fmt.Errorf("failed to scan binlog_expire_logs_seconds: %w", err)
		}
	}
	if expireSeconds <= 86400 {
		return errors.New("binlog_expire_logs_seconds is too low. Must be greater than 1 day")
	}

	// Check binlog_format
	rows, err = c.Execute(ctx, "SELECT @@binlog_format")
	if err != nil || len(rows) == 0 {
		return fmt.Errorf("failed to retrieve binlog_format: %w", err)
	}
	binlogFormat, _ := rows[0][0].(string)
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW'")
	}	

	// Check binlog_row_metadata 
	// rows, err := c.Execute(ctx, "SELECT @@binlog_row_metadata")
	// if err != nil {
	// 	return fmt.Errorf("failed to retrieve binlog_row_metadata: %w", err)
	// }
	// defer rows.Close()

	// var binlogRowMetadata string
	// if rows.Next() {
	// 	if err := rows.Scan(&binlogRowMetadata); err != nil {
	// 		return fmt.Errorf("failed to scan binlog_row_metadata: %w", err)
	// 	}
	// }

	// if binlogRowMetadata != "FULL" {
	// 	return errors.New("binlog_row_metadata must be set to 'FULL' for column exclusion support")
	// }	

	// Check binlog_row_image
	// rows, err := c.Execute(ctx, "SELECT @@binlog_row_image")
	// if err != nil {
	// 	return fmt.Errorf("failed to retrieve binlog_row_image: %w", err)
	// }
	// defer rows.Close()

	// var binlogRowImage string
	// if rows.Next() {
	// 	if err := rows.Scan(&binlogRowImage); err != nil {
	// 		return fmt.Errorf("failed to scan binlog_row_image: %w", err)
	// 	}
	// }

	// if binlogRowImage != "FULL" {
	// 	return errors.New("binlog_row_image must be set to 'FULL' (equivalent to PostgreSQL's REPLICA IDENTITY FULL)")
	// }


	// Check binlog_row_value_options
	rows, err = c.Execute(ctx, "SELECT @@binlog_row_value_options")
	if err != nil {
		return fmt.Errorf("failed to retrieve binlog_row_value_options: %w", err)
	}
	defer rows.Close()

	var binlogRowValueOptions string
	if rows.Next() {
		if err := rows.Scan(&binlogRowValueOptions); err != nil {
			return fmt.Errorf("failed to scan binlog_row_value_options: %w", err)
		}
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
