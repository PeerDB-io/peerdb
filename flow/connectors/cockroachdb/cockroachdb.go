package conncockroachdb

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type ReplState struct {
	Slot        string
	Publication string
	Offset      int64
	LastOffset  atomic.Int64
}

type CockroachDBConnector struct {
	logger                 log.Logger
	customTypeMapping      map[uint32]shared.CustomDataType
	ssh                    *utils.SSHTunnel
	conn                   *pgx.Conn
	replConn               *pgx.Conn
	replState              *ReplState
	Config                 *protos.CockroachDBConfig
	hushWarnOID            map[uint32]struct{}
	relationMessageMapping model.RelationMessageMapping
	typeMap                *pgtype.Map
	connStr                string
	metadataSchema         string
	replLock               sync.Mutex
	crdbVersion            string
}

func NewCockroachDBConnector(ctx context.Context, env map[string]string, config *protos.CockroachDBConfig) (*CockroachDBConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	flowNameInApplicationName, err := internal.PeerDBApplicationNamePerMirrorName(ctx, nil)
	if err != nil {
		logger.Error("Failed to get flow name from application name", slog.Any("error", err))
	}
	var flowName string
	if flowNameInApplicationName {
		flowName, _ = ctx.Value(shared.FlowNameKey).(string)
	}
	connectionString := GetCRDBConnectionString(config, flowName)
	connConfig, err := ParseConfig(connectionString, config)
	if err != nil {
		return nil, err
	}

	connConfig.Config.RuntimeParams["timezone"] = "UTC"
	connConfig.Config.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	connConfig.Config.RuntimeParams["statement_timeout"] = "0"
	connConfig.Config.RuntimeParams["DateStyle"] = "ISO, DMY"

	tunnel, err := utils.NewSSHTunnel(ctx, config.SshConfig)
	if err != nil {
		logger.Error("failed to create ssh tunnel", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}

	conn, err := NewCockroachDBConnFromConfig(ctx, connConfig, config.TlsHost, tunnel)
	if err != nil {
		tunnel.Close()
		logger.Error("failed to create connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	metadataSchema := "_peerdb_internal"
	if config.MetadataSchema != nil {
		metadataSchema = *config.MetadataSchema
	}

	return &CockroachDBConnector{
		logger:                 logger,
		Config:                 config,
		ssh:                    tunnel,
		conn:                   conn,
		replConn:               nil,
		replState:              nil,
		customTypeMapping:      nil,
		hushWarnOID:            make(map[uint32]struct{}),
		relationMessageMapping: make(model.RelationMessageMapping),
		connStr:                connectionString,
		metadataSchema:         metadataSchema,
		replLock:               sync.Mutex{},
		crdbVersion:            "",
		typeMap:                pgtype.NewMap(),
	}, nil
}

func (c *CockroachDBConnector) Close() error {
	var errs []error
	if c.replConn != nil {
		if err := c.replConn.Close(context.Background()); err != nil {
			c.logger.Error("failed to close replication connection", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to close replication connection: %w", err))
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(context.Background()); err != nil {
			c.logger.Error("failed to close connection", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
	}
	if err := c.ssh.Close(); err != nil {
		c.logger.Error("failed to close SSH tunnel", slog.Any("error", err))
		errs = append(errs, fmt.Errorf("failed to close SSH tunnel: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing CockroachDB connector: %v", errs)
	}
	return nil
}

func (c *CockroachDBConnector) ConnectionActive(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	return c.conn.Ping(ctx)
}

func (c *CockroachDBConnector) GetVersion(ctx context.Context) (string, error) {
	if c.crdbVersion != "" {
		return c.crdbVersion, nil
	}
	var version string
	err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get CockroachDB version: %w", err)
	}
	c.crdbVersion = version
	return version, nil
}
