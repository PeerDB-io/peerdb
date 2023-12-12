package connpostgres

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
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
		clientConfig, err = utils.GetSSHClientConfig(
			sshConfig.User,
			sshConfig.Password,
			sshConfig.PrivateKey,
		)
		if err != nil {
			logrus.Error("Failed to get SSH client config: ", err)
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
			logrus.Errorf("Failed to create pool: %v", err)
			return
		}

		logrus.Infof("Established pool to %s:%d",
			swpp.poolConfig.ConnConfig.Host, swpp.poolConfig.ConnConfig.Port)

		err = retryWithBackoff(func() error {
			err = swpp.Ping(swpp.ctx)
			if err != nil {
				logrus.Errorf("Failed to ping pool: %v", err)
				return err
			}
			return nil
		}, 5, 5*time.Second)

		if err != nil {
			logrus.Errorf("Failed to create pool: %v", err)
		}
	})

	if err == nil {
		logrus.Info("Successfully connected to Postgres")
	}

	return err
}

func (swpp *SSHWrappedPostgresPool) setupSSH() error {
	if swpp.sshConfig == nil {
		logrus.Info("SSH config is nil, skipping SSH setup")
		return nil
	}

	logrus.Info("Setting up SSH connection to ", swpp.sshServer)

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

func retryWithBackoff(fn retryFunc, maxRetries int, backoff time.Duration) (err error) {
	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if i < maxRetries-1 {
			logrus.Infof("Attempt #%d failed, retrying in %s", i+1, backoff)
			time.Sleep(backoff)
		}
	}
	return err
}

// see: https://github.com/jackc/pgx/issues/382#issuecomment-1496586216
type noDeadlineConn struct{ net.Conn }

func (c *noDeadlineConn) SetDeadline(t time.Time) error      { return nil }
func (c *noDeadlineConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *noDeadlineConn) SetWriteDeadline(t time.Time) error { return nil }
