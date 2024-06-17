package connpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
)

func (c *PostgresConnector) CheckSourceTables(ctx context.Context,
	tableNames []*utils.SchemaTable, pubName string,
) error {
	if c.conn == nil {
		return errors.New("check tables: conn is nil")
	}

	err := c.CheckIfTablesAreMirrorable(ctx, tableNames)
	if err != nil {
		return err
	}

	// Check that we can select from all tables
	tableArr := make([]string, 0, len(tableNames))
	for _, parsedTable := range tableNames {
		var row pgx.Row
		tableArr = append(tableArr, fmt.Sprintf(`(%s::text, %s::text)`,
			QuoteLiteral(parsedTable.Schema), QuoteLiteral(parsedTable.Table)))
		err := c.conn.QueryRow(ctx,
			fmt.Sprintf("SELECT * FROM %s.%s LIMIT 0;",
				QuoteIdentifier(parsedTable.Schema), QuoteIdentifier(parsedTable.Table))).Scan(&row)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}
	}

	tableStr := strings.Join(tableArr, ",")

	if pubName != "" {
		// Check if publication exists
		err := c.conn.QueryRow(ctx, "SELECT pubname FROM pg_publication WHERE pubname=$1", pubName).Scan(nil)
		if err != nil {
			if err == pgx.ErrNoRows {
				return fmt.Errorf("publication does not exist: %s", pubName)
			}
			return fmt.Errorf("error while checking for publication existence: %w", err)
		}

		// Check if tables belong to publication
		var pubTableCount int
		err = c.conn.QueryRow(ctx, fmt.Sprintf(`
		with source_table_components (sname, tname) as (values %s)
		select COUNT(DISTINCT(schemaname,tablename)) from pg_publication_tables
		INNER JOIN source_table_components stc
		ON schemaname=stc.sname and tablename=stc.tname where pubname=$1;`, tableStr), pubName).Scan(&pubTableCount)
		if err != nil {
			return err
		}

		if pubTableCount != len(tableNames) {
			return errors.New("not all tables belong to publication")
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
		if err == pgx.ErrNoRows {
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
		if err != pgx.ErrNoRows {
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
	var maxWalSendersRes string
	err = c.conn.QueryRow(ctx, "SHOW max_wal_senders").Scan(&maxWalSendersRes)
	if err != nil {
		return err
	}

	maxWalSenders, err := strconv.Atoi(maxWalSendersRes)
	if err != nil {
		return err
	}

	if maxWalSenders < 2 {
		return errors.New("max_wal_senders must be at least 2")
	}

	return nil
}

func (c *PostgresConnector) CheckReplicationConnectivity(ctx context.Context) error {
	// Check if we can create a replication connection
	conn, err := c.CreateReplConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %v", err)
	}

	return conn.Close(ctx)
}

// Check if all tables have either a primary key or REPLICA IDENTITY
func (c *PostgresConnector) CheckIfTablesAreMirrorable(
	ctx context.Context,
	tables []*utils.SchemaTable,
) error {
	var badTables []string
	for _, table := range tables {
		var pkeyOrReplIdentFull sql.NullBool
		err := c.conn.QueryRow(ctx, `SELECT
		(con.contype = 'p' OR t.relreplident in ('f')) AS can_mirror
		FROM pg_class t
		LEFT JOIN pg_namespace n ON t.relnamespace = n.oid
		LEFT JOIN pg_constraint con ON con.conrelid = t.oid
		WHERE n.nspname = $1 AND t.relname = $2;
		`, table.Schema, table.Table).Scan(&pkeyOrReplIdentFull)
		if err != nil {
			slog.Info("failed to check if table can be mirrored", slog.Any("error", err))
			return err
		}

		if !pkeyOrReplIdentFull.Valid || !pkeyOrReplIdentFull.Bool {
			// Check for replica identity index
			relId, err := c.getRelIDForTable(ctx, table)
			if err != nil {
				return err
			}

			indexCols, err := c.getReplicaIdentityIndexColumns(ctx, relId, table)
			if err != nil {
				return err
			}

			if len(indexCols) == 0 {
				badTables = append(badTables, fmt.Sprintf("%s.%s", table.Schema, table.Table))
			}
		}
	}

	if len(badTables) == 0 {
		return nil
	}

	return fmt.Errorf("tables %s neither have a primary key nor valid REPLICA IDENTITY INDEX/FULL", strings.Join(badTables, ","))
}
