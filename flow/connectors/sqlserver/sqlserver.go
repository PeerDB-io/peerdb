package connsqlserver

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"go.temporal.io/sdk/log"

	peersql "github.com/PeerDB-io/peer-flow/connectors/sql"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type SQLServerConnector struct {
	peersql.GenericSQLQueryExecutor

	config *protos.SqlServerConfig
	db     *sqlx.DB
	logger log.Logger
}

// NewSQLServerConnector creates a new SQL Server connection
func NewSQLServerConnector(ctx context.Context, config *protos.SqlServerConfig) (*SQLServerConnector, error) {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		config.Server, config.User, config.Password, config.Port, config.Database)

	db, err := sqlx.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	logger := shared.LoggerFromCtx(ctx)

	genericExecutor := *peersql.NewGenericSQLQueryExecutor(
		logger, db, sqlServerTypeToQValueKindMap, qValueKindToSQLServerTypeMap)

	return &SQLServerConnector{
		GenericSQLQueryExecutor: genericExecutor,
		config:                  config,
		db:                      db,
		logger:                  logger,
	}, nil
}

// Close closes the database connection
func (c *SQLServerConnector) Close() error {
	if c != nil {
		return c.db.Close()
	}
	return nil
}

// ConnectionActive checks if the connection is still active
func (c *SQLServerConnector) ConnectionActive(ctx context.Context) error {
	if err := c.db.PingContext(ctx); err != nil {
		return err
	}
	return nil
}
