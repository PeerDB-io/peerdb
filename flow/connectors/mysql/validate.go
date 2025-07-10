package connmysql

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	peerdb_mysql "github.com/PeerDB-io/peerdb/flow/shared/mysql"
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
	// GTID -> check GTID and error out if not enabled, check filepos as well
	// AUTO -> check GTID and fall back to filepos check
	// FILEPOS -> check filepos only
	if c.config.ReplicationMechanism != protos.MySqlReplicationMechanism_MYSQL_FILEPOS {
		if _, err := c.GetMasterGTIDSet(ctx); err != nil {
			if c.config.ReplicationMechanism == protos.MySqlReplicationMechanism_MYSQL_GTID {
				return fmt.Errorf("failed to check replication status: %w", err)
			}
		}
	}
	if namePos, err := c.GetMasterPos(ctx); err != nil {
		return fmt.Errorf("failed to check replication status: %w", err)
	} else if namePos.Name == "" || namePos.Pos <= 0 {
		return errors.New("invalid replication status: missing log file or position")
	}

	return nil
}

func (c *MySqlConnector) CheckBinlogSettings(ctx context.Context, requireRowMetadata bool) error {
	for conn, err := range c.withRetries(ctx) {
		if err != nil {
			return err
		}

		switch c.config.Flavor {
		case protos.MySqlFlavor_MYSQL_MARIA:
			return peerdb_mysql.CheckMariaDBBinlogSettings(conn, c.logger)
		case protos.MySqlFlavor_MYSQL_MYSQL:
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
				return peerdb_mysql.CheckMySQL5BinlogSettings(conn, c.logger)
			} else {
				return peerdb_mysql.CheckMySQL8BinlogSettings(conn, c.logger)
			}
		default:
			return fmt.Errorf("unsupported MySQL flavor: %s", c.config.Flavor.String())
		}
	}
	return errors.New("failed to connect to MySQL server")
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

	if err := c.CheckSourceTables(ctx, sourceTables); err != nil {
		return fmt.Errorf("provided source tables invalidated: %w", err)
	}
	// no need to check replication stuff for initial snapshot only mirrors
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	if err := c.CheckReplicationConnectivity(ctx); err != nil {
		return fmt.Errorf("unable to establish replication connectivity: %w", err)
	}

	for conn, err := range c.withRetries(ctx) {
		if err != nil {
			return err
		}

		if isVitess, err := peerdb_mysql.IsVitess(conn); err != nil {
			return err
		} else if isVitess && !(cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly) {
			return errors.New("vitess is currently not supported for MySQL mirrors in CDC")
		}
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
	for conn, err := range c.withRetries(ctx) {
		if err != nil {
			return err
		}
		if err := peerdb_mysql.CheckRDSBinlogSettings(conn, c.logger); err != nil {
			return fmt.Errorf("binlog configuration error: %w", err)
		}
	}

	return nil
}

func (c *MySqlConnector) ValidateCheck(ctx context.Context) error {
	if c.config.Flavor == protos.MySqlFlavor_MYSQL_UNKNOWN {
		return errors.New("flavor is set to unknown")
	}

	for conn, err := range c.withRetries(ctx) {
		if err != nil {
			return err
		}
		return c.validateFlavor(conn)
	}
	return errors.New("failed to connect to MySQL server")
}

func (c *MySqlConnector) validateFlavor(conn *client.Conn) error {
	// MariaDB specific setting, introduced in MariaDB 10.0.3
	if rs, err := conn.Execute("SELECT @@gtid_strict_mode"); err != nil {
		var mErr *mysql.MyError
		// seems to be MySQL
		if errors.As(err, &mErr) && mErr.Code == mysql.ER_UNKNOWN_SYSTEM_VARIABLE {
			if c.config.Flavor != protos.MySqlFlavor_MYSQL_MYSQL {
				return errors.New("server appears to be MySQL but MariaDB source has been selected")
			}
		} else {
			return fmt.Errorf("failed to check GTID mode: %w", err)
		}
	} else if len(rs.Values) > 0 {
		if c.config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
			return errors.New("server appears to be MariaDB but MySQL source has been selected")
		}
	}

	return nil
}
