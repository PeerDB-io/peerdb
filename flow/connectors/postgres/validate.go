package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (c *PostgresConnector) CheckSourceTables(ctx context.Context,
	tableNames []*utils.SchemaTable, pubName string, noCDC bool,
) error {
	if c.conn == nil {
		return errors.New("check tables: conn is nil")
	}

	// Check that we can select from all tables
	tableArr := make([]string, 0, len(tableNames))
	for _, parsedTable := range tableNames {
		var row pgx.Row
		tableArr = append(tableArr, fmt.Sprintf(`(%s::text,%s::text)`,
			QuoteLiteral(parsedTable.Schema), QuoteLiteral(parsedTable.Table)))
		if err := c.conn.QueryRow(ctx,
			fmt.Sprintf("SELECT * FROM %s.%s LIMIT 0", QuoteIdentifier(parsedTable.Schema), QuoteIdentifier(parsedTable.Table)),
		).Scan(&row); err != nil && err != pgx.ErrNoRows {
			return err
		}
	}

	if pubName != "" && !noCDC {
		// Check if publication exists
		var alltables bool
		if err := c.conn.QueryRow(ctx, "SELECT puballtables FROM pg_publication WHERE pubname=$1", pubName).Scan(&alltables); err != nil {
			if err == pgx.ErrNoRows {
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
				return fmt.Sprintf("%s.%s", QuoteIdentifier(schema), QuoteIdentifier(table)), nil
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
