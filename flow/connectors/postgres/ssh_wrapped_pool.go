package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/log"
	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
)

type SSHTunnel struct {
	sshConfig *ssh.ClientConfig
	sshServer string
	sshClient *ssh.Client
}

func NewSSHTunnel(
	ctx context.Context,
	sshConfig *protos.SSHConfig,
) (*SSHTunnel, error) {
	var sshServer string
	var clientConfig *ssh.ClientConfig

	if sshConfig != nil {
		sshServer = fmt.Sprintf("%s:%d", sshConfig.Host, sshConfig.Port)
		var err error
		clientConfig, err = utils.GetSSHClientConfig(sshConfig)
		if err != nil {
			logger.LoggerFromCtx(ctx).Error("Failed to get SSH client config", "error", err)
			return nil, err
		}
	}

	tunnel := &SSHTunnel{
		sshConfig: clientConfig,
		sshServer: sshServer,
	}

	err := tunnel.setupSSH(logger.LoggerFromCtx(ctx))
	if err != nil {
		return nil, err
	}

	return tunnel, nil
}

func (tunnel *SSHTunnel) setupSSH(logger log.Logger) error {
	if tunnel.sshConfig == nil {
		return nil
	}

	logger.Info("Setting up SSH connection to " + tunnel.sshServer)

	var err error
	tunnel.sshClient, err = ssh.Dial("tcp", tunnel.sshServer, tunnel.sshConfig)
	if err != nil {
		return err
	}

	return nil
}

func (tunnel *SSHTunnel) Close() {
	if tunnel.sshClient != nil {
		tunnel.sshClient.Close()
	}
}

func (tunnel *SSHTunnel) NewPostgresConnFromPostgresConfig(
	ctx context.Context,
	pgConfig *protos.PostgresConfig,
) (*pgx.Conn, error) {
	connectionString := utils.GetPGConnectionString(pgConfig)

	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}
	connConfig.RuntimeParams["application_name"] = "peerdb"

	return tunnel.NewPostgresConnFromConfig(ctx, connConfig)
}

func (tunnel *SSHTunnel) NewPostgresConnFromConfig(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
) (*pgx.Conn, error) {
	if tunnel.sshClient != nil {
		connConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := tunnel.sshClient.Dial(network, addr)
			if err != nil {
				return nil, err
			}
			return &noDeadlineConn{Conn: conn}, nil
		}
	}

	logger := logger.LoggerFromCtx(ctx)
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		logger.Error("Failed to create pool:", slog.Any("error", err))
		return nil, err
	}

	host := connConfig.Host
	err = retryWithBackoff(logger, func() error {
		err = conn.Ping(ctx)
		if err != nil {
			logger.Error("Failed to ping pool", slog.Any("error", err), slog.String("host", host))
			return err
		}
		return nil
	}, 5, 5*time.Second)

	if err != nil {
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
