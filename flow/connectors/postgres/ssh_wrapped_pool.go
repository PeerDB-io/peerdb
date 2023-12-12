package connpostgres

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

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
	localPort  uint16
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
			return nil, err
		}
	}

	return &SSHWrappedPostgresPool{
		poolConfig: poolConfig,
		sshConfig:  clientConfig,
		sshServer:  sshServer,
		ctx:        swCtx,
		cancel:     cancel,
	}, nil
}

func (swpp *SSHWrappedPostgresPool) Connect() error {
	var err error
	swpp.once.Do(func() {
		err = swpp.setupSSH(swpp.ctx)
		if err != nil {
			return
		}
		swpp.Pool, err = pgxpool.NewWithConfig(swpp.ctx, swpp.poolConfig)
	})
	return err
}

func (swpp *SSHWrappedPostgresPool) setupSSH(ctx context.Context) error {
	if swpp.sshClient == nil {
		logrus.Info("SSH client is nil, skipping SSH setup")
		return nil
	}

	logrus.Info("Setting up SSH connection to ", swpp.sshServer)
	var err error

	// Establish an SSH connection
	swpp.sshClient, err = ssh.Dial("tcp", swpp.sshServer, swpp.sshConfig)
	if err != nil {
		return err
	}

	// Automatically pick an available local port
	localListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return err
	}
	addr := localListener.Addr()
	swpp.localPort = uint16(addr.(*net.TCPAddr).Port)

	go func() {
		defer localListener.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				localConn, err := localListener.Accept()
				if err != nil {
					return
				}

				remoteConn, err := swpp.sshClient.Dial("tcp", "localhost:5432")
				if err != nil {
					localConn.Close()
					return
				}

				go func() {
					defer localConn.Close()
					defer remoteConn.Close()
					select {
					case <-ctx.Done():
						return
					default:
						io.Copy(localConn, remoteConn)
						io.Copy(remoteConn, localConn)
					}
				}()
			}
		}
	}()

	// Update the connection string to use the dynamically assigned local port
	swpp.poolConfig.ConnConfig.Host = "localhost"
	swpp.poolConfig.ConnConfig.Port = swpp.localPort

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
