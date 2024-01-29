package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type SSHTunnel struct {
	sshConfig *ssh.ClientConfig
	sshServer string
	once      sync.Once
	sshClient *ssh.Client
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewSSHTunnel(
	ctx context.Context,
	sshConfig *protos.SSHConfig,
) (*SSHTunnel, error) {
	swCtx, cancel := context.WithCancel(ctx)

	var sshServer string
	var clientConfig *ssh.ClientConfig

	if sshConfig != nil {
		sshServer = fmt.Sprintf("%s:%d", sshConfig.Host, sshConfig.Port)
		var err error
		clientConfig, err = utils.GetSSHClientConfig(sshConfig)
		if err != nil {
			slog.Error("Failed to get SSH client config", slog.Any("error", err))
			cancel()
			return nil, err
		}
	}

	pool := &SSHTunnel{
		sshConfig: clientConfig,
		sshServer: sshServer,
		ctx:       swCtx,
		cancel:    cancel,
	}

	err := pool.connect()
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func (tunnel *SSHTunnel) connect() error {
	var err error
	tunnel.once.Do(func() {
		err = tunnel.setupSSH()
	})

	return err
}

func (tunnel *SSHTunnel) setupSSH() error {
	if tunnel.sshConfig == nil {
		return nil
	}

	slog.Info("Setting up SSH connection to " + tunnel.sshServer)

	var err error
	tunnel.sshClient, err = ssh.Dial("tcp", tunnel.sshServer, tunnel.sshConfig)
	if err != nil {
		return err
	}

	return nil
}

func (tunnel *SSHTunnel) Close() {
	tunnel.cancel()

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

	conn, err := pgx.ConnectConfig(tunnel.ctx, connConfig)
	if err != nil {
		slog.Error("Failed to create pool:", slog.Any("error", err))
		return nil, err
	}

	host := connConfig.Host
	err = retryWithBackoff(func() error {
		err = conn.Ping(tunnel.ctx)
		if err != nil {
			slog.Error("Failed to ping pool", slog.Any("error", err), slog.String("host", host))
			return err
		}
		return nil
	}, 5, 5*time.Second)

	if err != nil {
		slog.Error("Failed to create pool", slog.Any("error", err), slog.String("host", host))
		conn.Close(ctx)
		return nil, err
	}

	return conn, nil
}

type retryFunc func() error

func retryWithBackoff(fn retryFunc, maxRetries int, backoff time.Duration) error {
	i := 0
	for {
		err := fn()
		if err == nil {
			return nil
		}
		i += 1
		if i < maxRetries {
			slog.Info(fmt.Sprintf("Attempt #%d failed, retrying in %s", i+1, backoff))
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
