package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *PostgresConnector) ValidateCheck(ctx context.Context) error {
	pgversion, err := c.MajorVersion(ctx)
	if err != nil {
		return err
	}

	if pgversion < shared.POSTGRES_12 {
		return fmt.Errorf("postgres must be of PG12 or above. Current version: %d", pgversion)
	}
	return nil
}

func (c *PostgresConnector) CheckSourceTables(
	ctx context.Context,
	tableNames []*utils.SchemaTable,
	tableMappings []*protos.TableMapping,
	pubName string,
	noCDC bool,
) error {
	if c.conn == nil {
		return errors.New("check tables: conn is nil")
	}

	// Check that we can select from all tables
	tableArr := make([]string, 0, len(tableNames))
	for idx, parsedTable := range tableNames {
		var row pgx.Row
		tableArr = append(tableArr, fmt.Sprintf(`(%s::text,%s::text)`,
			utils.QuoteLiteral(parsedTable.Schema), utils.QuoteLiteral(parsedTable.Table)))

		selectedColumnsStr := "*"
		if excludedColumns := tableMappings[idx].Exclude; len(excludedColumns) != 0 {
			selectedColumns, err := c.GetSelectedColumns(ctx, parsedTable, excludedColumns)
			if err != nil {
				return fmt.Errorf("failed to get selected columns for SELECT check: %w", err)
			}

			for i, col := range selectedColumns {
				selectedColumns[i] = utils.QuoteIdentifier(col)
			}

			selectedColumnsStr = strings.Join(selectedColumns, ", ")
		}
		if err := c.conn.QueryRow(ctx,
			fmt.Sprintf("SELECT %s FROM %s LIMIT 0", selectedColumnsStr, parsedTable),
		).Scan(&row); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("failed to select from table %s: %w", parsedTable, err)
		}
	}

	if pubName != "" && !noCDC {
		// Check if publication exists
		var alltables bool
		if err := c.conn.QueryRow(ctx, "SELECT puballtables FROM pg_publication WHERE pubname=$1", pubName).Scan(&alltables); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("publication does not exist: %s", pubName)
			}
			return fmt.Errorf("error while checking for publication existence: %w", err)
		}

		if !alltables {
			// Check if tables belong to publication
			tableStr := strings.Join(tableArr, ",")

			rows, err := c.conn.Query(
				ctx,
				fmt.Sprintf(`select schemaname,tablename
				from (values %s) as input(schemaname,tablename)
				where not exists (
					select * from pg_publication_tables pub
					where pubname=$1 and pub.schemaname=input.schemaname and pub.tablename=input.tablename
				)`, tableStr),
				pubName,
			)
			if err != nil {
				return err
			}
			missing, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
				var schema string
				var table string
				if err := row.Scan(&schema, &table); err != nil {
					return "", err
				}
				return fmt.Sprintf("%s.%s", utils.QuoteIdentifier(schema), utils.QuoteIdentifier(table)), nil
			})
			if err != nil {
				return err
			}

			if len(missing) != 0 {
				return errors.New("some tables missing from publication: " + strings.Join(missing, ", "))
			}
		}
	}

	return nil
}

func (c *PostgresConnector) CheckReplicationPermissions(ctx context.Context, username string) error {
	if c.conn == nil {
		return errors.New("check replication permissions: conn is nil")
	}

	var replicationRes bool
	err := c.conn.QueryRow(ctx, "SELECT rolreplication FROM pg_roles WHERE rolname = $1", username).Scan(&replicationRes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.logger.Warn("No rows in pg_roles for user. Skipping rolereplication check",
				"username", username)
		} else {
			return err
		}
	}

	if !replicationRes {
		// RDS case: check pg_settings for rds.logical_replication
		var setting string
		err := c.conn.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name = 'rds.logical_replication'").Scan(&setting)
		if !errors.Is(err, pgx.ErrNoRows) {
			if err != nil || setting != "on" {
				return errors.New("postgres user does not have replication role")
			}
		}
	}

	// check wal_level
	var walLevel string
	err = c.conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel)
	if err != nil {
		return err
	}

	if walLevel != "logical" {
		return errors.New("wal_level is not logical")
	}

	// max_wal_senders must be at least 2
	var insufficientMaxWalSenders bool
	err = c.conn.QueryRow(ctx,
		"SELECT setting::int<2 FROM pg_settings WHERE name='max_wal_senders'").Scan(&insufficientMaxWalSenders)
	if err != nil {
		return err
	}

	if insufficientMaxWalSenders {
		return errors.New("max_wal_senders must be at least 2")
	}

	serverVersion, err := shared.GetMajorVersion(ctx, c.conn)
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}

	var recoveryRes bool
	if err := c.conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&recoveryRes); err != nil {
		return fmt.Errorf("failed to check if Postgres is in recovery: %w", err)
	}
	if recoveryRes {
		if serverVersion < shared.POSTGRES_16 {
			return errors.New("cannot create replication slots on a standby server with version <16")
		}

		var hsFeedback bool
		if err := c.conn.QueryRow(
			ctx, "SELECT setting::bool FROM pg_settings WHERE name='hot_standby_feedback'",
		).Scan(&hsFeedback); err != nil {
			return fmt.Errorf("failed to check hot_standby_feedback: %w", err)
		}
		if !hsFeedback {
			return errors.New("hot_standby_feedback setting must be enabled on standby servers")
		}
	}

	return nil
}

func (c *PostgresConnector) CheckReplicationConnectivity(ctx context.Context) error {
	// Check if we can create a replication connection
	conn, err := c.CreateReplConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "IDENTIFY_SYSTEM"); err != nil {
		return fmt.Errorf("failed to execute IDENTIFY_SYSTEM on replication connection: %w", err)
	}

	return conn.Close(ctx)
}

func (c *PostgresConnector) CheckPublicationCreationPermissions(ctx context.Context, srcTableNames []string) error {
	pubName := "_peerdb_tmp_test_publication_" + shared.RandomString(5)
	if err := c.CreatePublication(ctx, srcTableNames, pubName); err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx, "DROP PUBLICATION "+pubName); err != nil {
		return fmt.Errorf("failed to drop publication: %v", err)
	}
	return nil
}

func (c *PostgresConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	noCDC := cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly
	if !noCDC {
		// Check replication connectivity
		if err := c.CheckReplicationConnectivity(ctx); err != nil {
			return fmt.Errorf("unable to establish replication connectivity: %w", err)
		}

		// Check permissions of postgres peer
		if err := c.CheckReplicationPermissions(ctx, c.Config.User); err != nil {
			return fmt.Errorf("failed to check replication permissions: %w", err)
		}
	}

	sourceTables := make([]*utils.SchemaTable, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		parsedTable, parseErr := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			return fmt.Errorf("invalid source table identifier: %w", parseErr)
		}

		sourceTables = append(sourceTables, parsedTable)
	}

	pubName := cfg.PublicationName
	if pubName == "" && !noCDC {
		srcTableNames := make([]string, 0, len(sourceTables))
		for _, srcTable := range sourceTables {
			srcTableNames = append(srcTableNames, srcTable.String())
		}

		if err := c.CheckPublicationCreationPermissions(ctx, srcTableNames); err != nil {
			return fmt.Errorf("invalid publication creation permissions: %w", err)
		}
	}

	if err := c.CheckSourceTables(ctx, sourceTables, cfg.TableMappings, pubName, noCDC); err != nil {
		return fmt.Errorf("provided source tables invalidated: %w", err)
	}

	return nil
}
