package connsqlserver

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"

	peersql "github.com/PeerDB-io/peer-flow/connectors/sql"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type SQLServerConnector struct {
	peersql.GenericSQLQueryExecutor

	ctx    context.Context
	config *protos.SqlServerConfig
	db     *sqlx.DB
	logger slog.Logger
}

// NewSQLServerConnector creates a new SQL Server connection
func NewSQLServerConnector(ctx context.Context, config *protos.SqlServerConfig) (*SQLServerConnector, error) {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		config.Server, config.User, config.Password, config.Port, config.Database)

	db, err := sqlx.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	genericExecutor := *peersql.NewGenericSQLQueryExecutor(
		ctx, db, sqlServerTypeToQValueKindMap, qValueKindToSQLServerTypeMap)

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)

	return &SQLServerConnector{
		GenericSQLQueryExecutor: genericExecutor,
		ctx:                     ctx,
		config:                  config,
		db:                      db,
		logger:                  *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
	}, nil
}

// Close closes the database connection
func (c *SQLServerConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// ConnectionActive checks if the connection is still active
func (c *SQLServerConnector) ConnectionActive() error {
	if err := c.db.Ping(); err != nil {
		return err
	}
	return nil
}
