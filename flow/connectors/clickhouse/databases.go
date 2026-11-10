package connclickhouse

import (
	"context"
	"fmt"
	"slices"

	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
)

var systemDatabases = []string{"system", "INFORMATION_SCHEMA", "information_schema"}

// ListDatabases returns the user databases of the ClickHouse server.
func (c *ClickHouseConnector) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := c.database.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to read database name: %w", err)
		}
		if !slices.Contains(systemDatabases, name) {
			databases = append(databases, name)
		}
	}
	return databases, rows.Err()
}

// CreateDatabaseIfNotExists creates a database, honouring the peer's cluster setting.
func (c *ClickHouseConnector) CreateDatabaseIfNotExists(ctx context.Context, database string) error {
	return c.execWithLogging(ctx,
		"CREATE DATABASE IF NOT EXISTS "+peerdb_clickhouse.QuoteIdentifier(database)+c.onCluster())
}
