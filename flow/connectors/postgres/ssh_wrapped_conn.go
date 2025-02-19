package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/peerdbenv"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func NewPostgresConnFromPostgresConfig(
	ctx context.Context,
	pgConfig *protos.PostgresConfig,
	tunnel utils.SSHTunnel,
) (*pgx.Conn, error) {
	flowNameInApplicationName, err := peerdbenv.PeerDBApplicationNamePerMirrorName(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Error("Failed to get flow name from application name", slog.Any("error", err))
	}

	var flowName string
	if flowNameInApplicationName {
		flowName, _ = ctx.Value(shared.FlowNameKey).(string)
	}
	connectionString := internal.GetPGConnectionString(pgConfig, flowName)

	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

	return NewPostgresConnFromConfig(ctx, connConfig, tunnel)
}

func NewPostgresConnFromConfig(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
	tunnel utils.SSHTunnel,
) (*pgx.Conn, error) {
	if tunnel.Client != nil {
		connConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := tunnel.Client.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &noDeadlineConn{Conn: conn}, nil
		}
		// DNS lookup seems to happen before connection is established which can be an issue if given host
		// can only be resolved on the SSH host https://github.com/jackc/pgx/issues/1724
		connConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			return []string{host}, nil
		}
	}

	logger := internal.LoggerFromCtx(ctx)
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		logger.Error("Failed to create pool", slog.Any("error", err))
		return nil, err
	}

	host := connConfig.Host
	if err := retryWithBackoff(logger, func() error {
		_, err := conn.Exec(ctx, "SELECT 1")
		if err != nil {
			logger.Error("Failed to ping pool", slog.Any("error", err), slog.String("host", host))
			return err
		}
		return nil
	}, 5, 5*time.Second); err != nil {
		logger.Error("Failed to create pool", slog.Any("error", err), slog.String("host", host))
		conn.Close(ctx)
		return nil, err
	}

	return conn, nil
}

type retryFunc func() error

func retryWithBackoff(logger log.Logger, fn retryFunc, maxRetries int, backoff time.Duration) error {
	i := 0
	for {
		err := fn()
		if err == nil {
			return nil
		}
		i += 1
		if i < maxRetries {
			logger.Info(fmt.Sprintf("Attempt #%d failed, retrying in %s", i+1, backoff))
			time.Sleep(backoff)
		} else {
			return err
		}
	}
}

// see: https://github.com/jackc/pgx/issues/382#issuecomment-1496586216
type noDeadlineConn struct{ net.Conn }

func (c *noDeadlineConn) SetDeadline(t time.Time) error      { return nil }
func (c *noDeadlineConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *noDeadlineConn) SetWriteDeadline(t time.Time) error { return nil }
