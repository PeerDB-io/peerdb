package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/ssh"
)

type SSHWrappedPostgresPool struct {
	*pgxpool.Pool

	poolConfig *pgxpool.Config
	sshConfig  *ssh.ClientConfig
	sshServer  string
	once       sync.Once
	sshClient  *ssh.Client
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewSSHWrappedPostgresPool(
	ctx context.Context,
	poolConfig *pgxpool.Config,
	sshConfig *protos.SSHConfig,
) (*SSHWrappedPostgresPool, error) {
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

	pool := &SSHWrappedPostgresPool{
		poolConfig: poolConfig,
		sshConfig:  clientConfig,
		sshServer:  sshServer,
		ctx:        swCtx,
		cancel:     cancel,
	}

	err := pool.connect()
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func (swpp *SSHWrappedPostgresPool) connect() error {
	var err error
	swpp.once.Do(func() {
		err = swpp.setupSSH()
		if err != nil {
			return
		}

		swpp.Pool, err = pgxpool.NewWithConfig(swpp.ctx, swpp.poolConfig)
		if err != nil {
			slog.Error("Failed to create pool:", slog.Any("error", err))
			return
		}

		host := swpp.poolConfig.ConnConfig.Host
		err = retryWithBackoff(func() error {
			err = swpp.Ping(swpp.ctx)
			if err != nil {
				slog.Error("Failed to ping pool", slog.Any("error", err), slog.String("host", host))
				return err
			}
			return nil
		}, 5, 5*time.Second)

		if err != nil {
			slog.Error("Failed to create pool", slog.Any("error", err), slog.String("host", host))
		}
	})

	return err
}

func (swpp *SSHWrappedPostgresPool) setupSSH() error {
	if swpp.sshConfig == nil {
		return nil
	}

	slog.Info("Setting up SSH connection to " + swpp.sshServer)

	var err error
	swpp.sshClient, err = ssh.Dial("tcp", swpp.sshServer, swpp.sshConfig)
	if err != nil {
		return err
	}

	swpp.poolConfig.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := swpp.sshClient.Dial(network, addr)
		if err != nil {
			return nil, err
		}
		return &noDeadlineConn{Conn: conn}, nil
	}

	return nil
}

func (swpp *SSHWrappedPostgresPool) Close() {
	swpp.cancel()

	if swpp.Pool != nil {
		swpp.Pool.Close()
	}

	if swpp.sshClient != nil {
		swpp.sshClient.Close()
	}
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
