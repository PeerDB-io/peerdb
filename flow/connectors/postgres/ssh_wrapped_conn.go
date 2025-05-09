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
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func NewPostgresConnFromConfig(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
	tlsHost string,
	rdsAuth *utils.RDSAuth,
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
	if rdsAuth != nil {
		host := connConfig.Host
		if tlsHost != "" {
			host = tlsHost
		}
		logger.Info("Setting up IAM auth for Postgres")
		token, err := utils.GetRDSToken(ctx, utils.RDSConnectionConfig{
			Host: host,
			Port: uint32(connConfig.Port),
			User: connConfig.User,
		}, rdsAuth, "POSTGRES")
		if err != nil {
			return nil, err
		}
		connConfig = connConfig.Copy()
		connConfig.Password = token
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		logger.Error("Failed to create pool", slog.Any("error", err))
		return nil, err
	}

	if err := retryWithBackoff(logger, func() error {
		_, err := conn.Exec(ctx, "SELECT 1")
		if err != nil {
			logger.Error("Failed to ping pool", slog.Any("error", err), slog.String("host", connConfig.Host))
			return err
		}
		return nil
	}, 5, 5*time.Second); err != nil {
		logger.Error("Failed to create pool", slog.Any("error", err), slog.String("host", connConfig.Host))
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
