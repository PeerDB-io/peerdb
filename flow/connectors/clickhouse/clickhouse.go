package connclickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickhouseConnector struct {
	ctx                context.Context
	database           *sql.DB
	tableSchemaMapping map[string]*protos.TableSchema
	logger             slog.Logger
}

func NewClickhouseConnector(ctx context.Context,
	clickhouseProtoConfig *protos.ClickhouseConfig,
) (*ClickhouseConnector, error) {
	database, err := connect(ctx, clickhouseProtoConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Clickhouse peer: %w", err)
	}

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return &ClickhouseConnector{
		ctx:                ctx,
		database:           database,
		tableSchemaMapping: nil,
		logger:             *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
	}, nil
}

func connect(ctx context.Context, config *protos.ClickhouseConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s", // TODO &database=%s"
		config.Host, config.Port, config.User, config.Password) // TODO , config.Database

	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Clickhouse peer: %w", err)
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping to Clickhouse peer: %w", err)
	}

	// Execute USE database command to select a specific database
	_, err = conn.Exec(fmt.Sprintf("USE %s", config.Database))
	if err != nil {
		return nil, fmt.Errorf("failed in selecting db in Clickhouse peer: %w", err)
	}

	return conn, nil
}

func (c *ClickhouseConnector) Close() error {
	if c == nil || c.database == nil {
		return nil
	}

	err := c.database.Close()
	if err != nil {
		return fmt.Errorf("error while closing connection to Clickhouse peer: %w", err)
	}
	return nil
}

func (c *ClickhouseConnector) ConnectionActive() error {
	if c == nil || c.database == nil {
		return fmt.Errorf("ClickhouseConnector is nil")
	}

	// This also checks if database exists
	err := c.database.PingContext(c.ctx)
	return err
}
