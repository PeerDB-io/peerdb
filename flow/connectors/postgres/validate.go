package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
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
				return fmt.Sprintf("%s.%s", common.QuoteIdentifier(schema), common.QuoteIdentifier(table)), nil
			})
			if err != nil {
				return err
			}

			if len(missing) != 0 {
				return fmt.Errorf("some tables missing from publication: %s", strings.Join(missing, ", "))
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
	pubName := "_peerdb_tmp_test_publication_" + shared.RandomString(5)
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

func (c *PostgresConnector) ValidateMirrorDestination(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) error {
	if cfg.Resync {
		return nil // no need to validate schema for resync, as we will create or replace the tables
	}
	// Validate that all source columns exist in destination tables
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

		// Parse destination table identifier
		dstTable, err := common.ParseTableIdentifier(dstTableIdentifier)
		if err != nil {
			return fmt.Errorf("invalid destination table identifier %s: %w", dstTableIdentifier, err)
		}

		// Get destination table columns
		dstColumns, err := c.getDestinationTableColumns(ctx, dstTable)
		if err != nil {
			// If table doesn't exist, that's fine - we'll create it
			if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), "does not exist") {
				continue
			}
			return fmt.Errorf("failed to get columns for destination table %s: %w", dstTableIdentifier, err)
		}

		// Build a set of destination column names for quick lookup
		dstColumnSet := make(map[string]struct{}, len(dstColumns))
		for _, col := range dstColumns {
			dstColumnSet[col] = struct{}{}
		}

		// Check if all source columns exist in destination
		for _, srcField := range srcSchema.Columns {
			colName := srcField.Name

			// Skip excluded columns
			if slices.Contains(tableMapping.Exclude, colName) {
				continue
			}

			// Check if column is renamed in the mapping
			for _, col := range tableMapping.Columns {
				if col.SourceName == colName && col.DestinationName != "" {
					colName = col.DestinationName
					break
				}
			}

			// Check if the column exists in destination
			if _, exists := dstColumnSet[colName]; !exists {
				return fmt.Errorf("source column %s (as %s) not found in destination table %s",
					srcField.Name, colName, dstTableIdentifier)
			}
		}
	}

	return nil
}

func (c *PostgresConnector) getDestinationTableColumns(ctx context.Context, table *common.QualifiedTable) ([]string, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT attname
		FROM pg_attribute
		JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
		JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
		WHERE pg_namespace.nspname = $1
			AND pg_class.relname = $2
			AND pg_attribute.attnum > 0
			AND NOT pg_attribute.attisdropped
	`, table.Namespace, table.Table)
	if err != nil {
		return nil, err
	}

	columns, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, pgx.ErrNoRows
	}

	return columns, nil
}
