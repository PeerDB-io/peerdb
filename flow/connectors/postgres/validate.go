package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	pg_validation "github.com/PeerDB-io/peerdb/flow/pkg/postgres"
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
	tableNames []*common.QualifiedTable,
	tableMappings []*protos.TableMapping,
	pubName string,
	noCDC bool,
) error {
	if c.conn == nil {
		return fmt.Errorf("check tables: conn is nil")
	}

	// Check that we can select from all tables
	tableArr := make([]string, 0, len(tableNames))
	var missingTables []common.QualifiedTable
	for idx, parsedTable := range tableNames {
		var row pgx.Row
		tableArr = append(tableArr, fmt.Sprintf(`(%s::text,%s::text)`,
			utils.QuoteLiteral(parsedTable.Namespace), utils.QuoteLiteral(parsedTable.Table)))

		selectedColumnsStr := "*"
		if excludedColumns := tableMappings[idx].Exclude; len(excludedColumns) != 0 {
			selectedColumns, err := c.GetSelectedColumns(ctx, parsedTable, excludedColumns)
			if err != nil {
				return fmt.Errorf("failed to get selected columns for SELECT check: %w", err)
			}

			for i, col := range selectedColumns {
				selectedColumns[i] = common.QuoteIdentifier(col)
			}

			selectedColumnsStr = strings.Join(selectedColumns, ", ")
		}
		if err := c.conn.QueryRow(ctx,
			fmt.Sprintf("SELECT %s FROM %s LIMIT 0", selectedColumnsStr, parsedTable),
		).Scan(&row); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == pgerrcode.UndefinedTable {
				missingTables = append(missingTables, *parsedTable)
				continue
			}
			return fmt.Errorf("failed to select from table %s: %w", parsedTable, err)
		}
	}
	if len(missingTables) > 0 {
		return common.NewSourceTablesMissingError(missingTables)
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
			missing, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (common.QualifiedTable, error) {
				var qt common.QualifiedTable
				if err := row.Scan(&qt.Namespace, &qt.Table); err != nil {
					return common.QualifiedTable{}, err
				}
				return qt, nil
			})
			if err != nil {
				return err
			}

			if len(missing) != 0 {
				return common.NewTablesNotInPublicationError(pubName, missing)
			}
		}
	}

	return nil
}

func (c *PostgresConnector) CheckReplicationPermissions(ctx context.Context, username string) error {
	if c.conn == nil {
		return fmt.Errorf("check replication permissions: conn is nil")
	}

	// check wal_level
	var walLevel string
	if err := c.conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return err
	}

	if walLevel != "logical" {
		return fmt.Errorf("wal_level is not logical")
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
				return fmt.Errorf("rds.logical_replication setting must be enabled")
			}
		}
	}

	// max_wal_senders must be at least 2
	var insufficientMaxWalSenders bool
	if err := c.conn.QueryRow(ctx,
		"SELECT setting::int<2 FROM pg_settings WHERE name='max_wal_senders'",
	).Scan(&insufficientMaxWalSenders); err != nil {
		return err
	}

	if insufficientMaxWalSenders {
		return fmt.Errorf("max_wal_senders must be at least 2")
	}

	serverVersion, err := shared.GetMajorVersion(ctx, c.conn)
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}

	if recoveryRes, err := c.IsInRecovery(ctx); err != nil {
		return fmt.Errorf("failed to check if Postgres is in recovery: %w", err)
	} else if recoveryRes {
		if serverVersion < shared.POSTGRES_16 {
			return fmt.Errorf("cannot create replication slots on a standby server with version <16")
		}

		var hsFeedback bool
		if err := c.conn.QueryRow(
			ctx, "SELECT setting::bool FROM pg_settings WHERE name='hot_standby_feedback'",
		).Scan(&hsFeedback); err != nil {
			return fmt.Errorf("failed to check hot_standby_feedback: %w", err)
		}
		if !hsFeedback {
			return fmt.Errorf("hot_standby_feedback setting must be enabled on standby servers")
		}
	}

	return nil
}

func (c *PostgresConnector) CheckReplicationConnectivity(ctx context.Context, env map[string]string) error {
	// Check if we can create a replication connection
	conn, err := c.CreateReplConn(ctx, env)
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
	pubName := "_peerdb_tmp_test_publication_" + common.RandomString(5)
	if err := c.CreatePublication(ctx, srcTableNames, pubName); err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx, "DROP PUBLICATION "+pubName); err != nil {
		return fmt.Errorf("failed to drop publication: %v", err)
	}
	return nil
}

func (c *PostgresConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	noCDC := cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly
	if !noCDC {
		// Check replication connectivity
		if err := c.CheckReplicationConnectivity(ctx, cfg.Env); err != nil {
			return fmt.Errorf("unable to establish replication connectivity: %w", err)
		}

		// Check permissions of postgres peer
		if err := c.CheckReplicationPermissions(ctx, c.Config.User); err != nil {
			return fmt.Errorf("failed to check replication permissions: %w", err)
		}
	}

	sourceTables := make([]*common.QualifiedTable, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		parsedTable, parseErr := common.ParseTableIdentifier(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			return fmt.Errorf("invalid source table identifier: %w", parseErr)
		}

		sourceTables = append(sourceTables, parsedTable)
	}

	pubName := cfg.PublicationName
	// Check source tables before the publication, for better errors
	if err := c.CheckSourceTables(ctx, sourceTables, cfg.TableMappings, pubName, noCDC); err != nil {
		return fmt.Errorf("provided source tables invalidated: %w", err)
	}

	if pubName == "" && !noCDC {
		srcTableNames := make([]string, 0, len(sourceTables))
		for _, srcTable := range sourceTables {
			srcTableNames = append(srcTableNames, srcTable.String())
		}

		if err := c.CheckPublicationCreationPermissions(ctx, srcTableNames); err != nil {
			return fmt.Errorf("invalid publication creation permissions: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) ValidateMirrorDestination(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) error {
	if cfg.Resync {
		return nil // no need to validate schema for resync, as we will create or replace the tables
	}

	// Validate that all source columns exist in destination tables
	checkedSchemas := make(map[string]struct{})
	for _, tableMapping := range cfg.TableMappings {
		srcTableIdentifier := tableMapping.SourceTableIdentifier
		dstTableIdentifier := tableMapping.DestinationTableIdentifier

		if dstTableIdentifier == "" {
			return errors.New("destination table identifier is empty")
		}

		// Get the source table schema
		srcSchema, ok := tableNameSchemaMapping[srcTableIdentifier]
		if !ok {
			return fmt.Errorf("source table %s not found in schema mapping", srcTableIdentifier)
		}

		// Get destination table columns with types
		dstColumns, err := pg_validation.GetDestinationTableSchema(ctx, c.conn, dstTableIdentifier)
		if err != nil {
			// If table doesn't exist, check that the schema does before continuing
			if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), "does not exist") {
				parsedDst, parseErr := common.ParseTableIdentifier(dstTableIdentifier)
				if parseErr != nil {
					return fmt.Errorf("invalid destination table identifier %s: %w", dstTableIdentifier, parseErr)
				}
				if _, alreadyChecked := checkedSchemas[parsedDst.Namespace]; !alreadyChecked {
					if schemaErr := pg_validation.CheckSchemaExists(ctx, c.conn, parsedDst.Namespace); schemaErr != nil {
						return schemaErr
					}
					checkedSchemas[parsedDst.Namespace] = struct{}{}
				}
				continue
			}
			return fmt.Errorf("failed to get columns for destination table %s: %w", dstTableIdentifier, err)
		}

		if cfg.DoInitialSnapshot {
			// Check if destination table already has rows
			if err := pg_validation.CheckTableEmpty(ctx, c.conn, dstTableIdentifier); err != nil {
				return err
			}
		}

		// Check if all source columns exist in destination with matching types
		for _, srcField := range srcSchema.Columns {
			colName := srcField.Name

			// Skip excluded columns
			if slices.Contains(tableMapping.Exclude, colName) {
				continue
			}

			// Resolve rename if present
			columnMappings := make([]pg_validation.ColumnMapping, 0, len(tableMapping.Columns))
			for _, col := range tableMapping.Columns {
				columnMappings = append(columnMappings, pg_validation.ColumnMapping{
					SourceName:      col.SourceName,
					DestinationName: col.DestinationName,
				})
			}
			colName = pg_validation.ResolveDestinationColumnName(colName, columnMappings)

			// Check if the column exists in destination
			if err := pg_validation.CheckColumnExists(srcField.Name, colName, dstColumns, dstTableIdentifier); err != nil {
				return err
			}

			// Check type compatibility when using the PG type system
			if cfg.System == protos.TypeSystem_PG {
				dstCol := dstColumns[colName]
				if err := pg_validation.CheckColumnTypeCompatibility(
					srcField.Name, srcField.Type, srcField.TypeModifier,
					colName, dstCol.TypeName, dstCol.TypeMod,
					dstTableIdentifier,
				); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
