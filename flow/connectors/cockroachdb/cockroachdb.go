package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type CockroachDBConnector struct {
	*metadataStore.PostgresMetadata
	logger      log.Logger
	ssh         *utils.SSHTunnel
	conn        *crdbConn
	config      *protos.CockroachDBConfig
	connStr     string
	crdbVersion string
	// set once the first changefeed session of this connector has released
	// the snapshot history retention job. Never reset: a connector instance
	// lives inside a single SyncFlow activity execution (flowable.go creates
	// it once and calls PullRecords in a loop until the activity ends), so
	// every session it runs resumes from the same flow's stored cursor
	// lineage, which only advances. A genuinely new cursor requires a new
	// SetupReplication (mirror create or resync), and that only happens
	// after the running CDC flow, its SyncFlow activity and therefore this
	// instance are torn down; the fresh instance starts with the flag false.
	// PullFlowCleanup releases the job independently as the backstop.
	historyProtectionChecked atomic.Bool
	// unix nanos of the last snapshot history protection extension fired by
	// this connector, throttling per-partition extension attempts
	historyExtendedAt atomic.Int64
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

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	tunnel, err := utils.NewSSHTunnel(ctx, config.SshConfig)
	if err != nil {
		logger.Error("failed to create ssh tunnel", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}

	conn, err := NewCockroachDBConnFromConfig(ctx, connConfig, tunnel)
	if err != nil {
		tunnel.Close()
		err = classifyConnectError(err)
		logger.Error("failed to create connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	connector := &CockroachDBConnector{
		PostgresMetadata: pgMetadata,
		logger:           logger,
		config:           config,
		ssh:              tunnel,
		conn:             conn,
		connStr:          connectionString,
		crdbVersion:      "",
	}

	tunnel.StartKeepalive(context.Background(), func() {
		connector.logger.Info("SSH keepalive failed, closing connection")
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := connector.conn.Close(closeCtx); err != nil {
			connector.logger.Error("failed to close CockroachDB connection on SSH keepalive failure", slog.Any("error", err))
		}
	})

	return connector, nil
}

// classifyConnectError wraps authentication and privilege failures in
// exceptions.AuthError like the postgres connector does, so drop flow skips
// source cleanup instead of hammering a peer whose credentials were rotated.
func classifyConnectError(err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pgErr.Code {
		case pgerrcode.InvalidAuthorizationSpecification,
			pgerrcode.InvalidPassword,
			pgerrcode.InsufficientPrivilege:
			return exceptions.NewAuthError(err)
		}
	}
	return err
}

func (c *CockroachDBConnector) Close() error {
	var errs []error
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
	return errors.Join(errs...)
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
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return "", fmt.Errorf("failed to get CockroachDB version: %w", err)
	}
	c.crdbVersion = version
	return version, nil
}
