package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresConnector) CheckSourceTables(ctx context.Context, tableNames []string, pubName string) error {
	if c.conn == nil {
		return errors.New("check tables: conn is nil")
	}

	// Check that we can select from all tables
	tableArr := make([]string, 0, len(tableNames))
	for _, table := range tableNames {
		var row pgx.Row
		schemaName, tableName, found := strings.Cut(table, ".")
		if !found {
			return fmt.Errorf("invalid source table identifier: %s", table)
		}

		tableArr = append(tableArr, fmt.Sprintf(`(%s::text, %s::text)`, QuoteLiteral(schemaName), QuoteLiteral(tableName)))
		err := c.conn.QueryRow(ctx,
			fmt.Sprintf("SELECT * FROM %s.%s LIMIT 0;", QuoteIdentifier(schemaName), QuoteIdentifier(tableName))).Scan(&row)
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

func (c *PostgresConnector) CheckReplicationConnectivity(ctx context.Context) error {
	// Check if we can create a replication connection
	conn, err := c.CreateReplConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %v", err)
	}

	var one int
	queryErr := conn.QueryRow(ctx, "SELECT 1").Scan(&one)
	if queryErr != nil {
		return fmt.Errorf("failed to query replication connection: %v", queryErr)
	}

	return nil
}
