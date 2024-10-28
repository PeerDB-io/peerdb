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
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type SSHTunnel struct {
	sshConfig *ssh.ClientConfig
	sshClient *ssh.Client
	sshServer string
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
			shared.LoggerFromCtx(ctx).Error("Failed to get SSH client config", "error", err)
			return nil, err
		}
	}

	tunnel := &SSHTunnel{
		sshConfig: clientConfig,
		sshServer: sshServer,
	}

	err := tunnel.setupSSH(shared.LoggerFromCtx(ctx))
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
	flowNameInApplicationName, err := peerdbenv.PeerDBApplicationNamePerMirrorName(ctx, nil)
	if err != nil {
		shared.LoggerFromCtx(ctx).Error("Failed to get flow name from application name", slog.Any("error", err))
	}

	var flowName string
	if flowNameInApplicationName {
		flowName, _ = ctx.Value(shared.FlowNameKey).(string)
	}
	connectionString := shared.GetPGConnectionString(pgConfig, flowName)

	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

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
		// DNS lookup seems to happen before connection is established which can be an issue if given host
		// can only be resolved on the SSH host https://github.com/jackc/pgx/issues/1724
		connConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			return []string{host}, nil
		}
	}

	logger := shared.LoggerFromCtx(ctx)
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		logger.Error("Failed to create pool", slog.Any("error", err))
		return nil, err
	}

	host := connConfig.Host
	err = retryWithBackoff(logger, func() error {
		_, err := conn.Exec(ctx, "SELECT 1")
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
