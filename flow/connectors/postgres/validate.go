package connpostgres

import (
	"context"
	"errors"
	"fmt"
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

	return nil
}

func (c *PostgresConnector) CheckReplicationPermissions(ctx context.Context, username string) error {
	if c.conn == nil {
		return errors.New("check replication permissions: conn is nil")
	}

	var replicationRes bool
	err := c.conn.QueryRow(ctx, "SELECT rolreplication FROM pg_roles WHERE rolname = $1", username).Scan(&replicationRes)
	if err != nil {
		return err
	}

	if !replicationRes {
		// RDS case: check pg_settings for rds.logical_replication
		var setting string
		err := c.conn.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name = 'rds.logical_replication'").Scan(&setting)
		if err != nil || setting != "on" {
			return errors.New("postgres user does not have replication role")
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

func (c *PostgresConnector) CheckPublicationPermission(ctx context.Context, tableNames []*utils.SchemaTable) error {
	var hasSuper bool
	var canCreateDatabase bool
	queryErr := c.conn.QueryRow(ctx, fmt.Sprintf(`
	SELECT
	rolsuper,
	has_database_privilege(rolname, current_database(), 'CREATE') AS can_create_database
	FROM pg_roles
	WHERE rolname = %s;
	`, QuoteLiteral(c.config.User))).Scan(&hasSuper, &canCreateDatabase)
	if queryErr != nil {
		return fmt.Errorf("error while checking user privileges: %w", queryErr)
	}

	if !hasSuper && !canCreateDatabase {
		return errors.New("user does not have superuser or create database privileges")
	}

	// for each table, check if the user is an owner
	for _, table := range tableNames {
		var owner string
		err := c.conn.QueryRow(ctx, fmt.Sprintf("SELECT tableowner FROM pg_tables WHERE schemaname=%s AND tablename=%s",
			QuoteLiteral(table.Schema), QuoteLiteral(table.Table))).Scan(&owner)
		if err != nil {
			return fmt.Errorf("error while checking table owner: %w", err)
		}

		if owner != c.config.User {
			return fmt.Errorf("user %s is not the owner of table %s", c.config.User, table.String())
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

	return conn.Close(ctx)
}
