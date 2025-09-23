package utils

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

const SSHKeepaliveInterval = 15 * time.Second

type SSHTunnel struct {
	*ssh.Client
	keepaliveChan atomic.Pointer[chan struct{}]
	badTunnel     bool
}

// GetSSHClientConfig returns an *ssh.ClientConfig based on provided credentials.
func GetSSHClientConfig(config *protos.SSHConfig) (*ssh.ClientConfig, error) {
	var authMethods []ssh.AuthMethod

	// Password-based authentication
	if config.Password != "" {
		authMethods = append(authMethods, ssh.Password(config.Password))
	}

	// Private key-based authentication
	if config.PrivateKey != "" {
		pkey, err := base64.StdEncoding.DecodeString(config.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to base64 decode private key: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(pkey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	if len(authMethods) == 0 {
		return nil, errors.New("no authentication methods provided")
	}

	var hostKeyCallback ssh.HostKeyCallback
	if config.HostKey != "" {
		pubKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(config.HostKey))
		if err != nil {
			return nil, fmt.Errorf("failed to parse host key: %w", err)
		}
		hostKeyCallback = ssh.FixedHostKey(pubKey)
	} else {
		//nolint:gosec
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	}

	return &ssh.ClientConfig{
		User:            config.User,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
	}, nil
}

func NewSSHTunnel(
	ctx context.Context,
	sshConfig *protos.SSHConfig,
) (*SSHTunnel, error) {
	if sshConfig != nil {
		logger := internal.LoggerFromCtx(ctx)
		sshServer := shared.JoinHostPort(sshConfig.Host, sshConfig.Port)
		clientConfig, err := GetSSHClientConfig(sshConfig)
		if err != nil {
			logger.Error("Failed to get SSH client config", "error", err)
			return nil, err
		}

		logger.Info("Setting up SSH connection", slog.String("Server", sshServer))
		client, err := ssh.Dial("tcp", sshServer, clientConfig)
		if err != nil {
			return nil, exceptions.NewSSHTunnelSetupError(err)
		}

		return &SSHTunnel{Client: client, badTunnel: false}, nil
	}

	return nil, nil
}

func (tunnel *SSHTunnel) Close() error {
	if tunnel != nil && tunnel.Client != nil {
		if keepaliveChan := tunnel.keepaliveChan.Swap(nil); keepaliveChan != nil {
			close(*keepaliveChan)
		}
		tunnel.badTunnel = true
		return tunnel.Client.Close()
	}
	return nil
}

// returns a channel that will receive a value if the SSH keepalive fails
// or nil if no SSH tunnel is configured
func (tunnel *SSHTunnel) GetKeepaliveChan(ctx context.Context) <-chan struct{} {
	if tunnel == nil || tunnel.Client == nil || tunnel.badTunnel {
		// nil channel would be of no consequence in a select
		// UNLESS it's the only branch in a select, in which case it would block forever
		return nil
	}
	if keepaliveChan := tunnel.keepaliveChan.Load(); keepaliveChan != nil {
		// Already started
		return *keepaliveChan
	}
	keepaliveChan := make(chan struct{})
	tunnel.keepaliveChan.Store(&keepaliveChan)

	go func() {
		ticker := time.NewTicker(SSHKeepaliveInterval)
		defer ticker.Stop()
		logger := internal.LoggerFromCtx(ctx)
		requestSent := false
		for {
			select {
			case <-ticker.C:
				if requestSent {
					// Previous keepalive request didn't return yet, something's wrong
					logger.Error("Previous keepalive request still pending, marking tunnel as bad")
					if keepaliveChan := tunnel.keepaliveChan.Swap(nil); keepaliveChan != nil {
						close(*keepaliveChan)
					}
					tunnel.badTunnel = true
					return
				}
				go func() {
					requestSent = true
					_, _, err := tunnel.Client.SendRequest("keepalive@openssh.com", true, nil)
					requestSent = false
					if err != nil {
						logger.Error("Failed to send keep alive", slog.Any("error", err))
						if keepaliveChan := tunnel.keepaliveChan.Swap(nil); keepaliveChan != nil {
							close(*keepaliveChan)
						}
						tunnel.badTunnel = true
						return
					}
				}()
			case <-ctx.Done():
				if keepaliveChan := tunnel.keepaliveChan.Swap(nil); keepaliveChan != nil {
					close(*keepaliveChan)
				}
				return
			case <-keepaliveChan:
				// channel closed from outside
				return
			}
		}
	}()
	return keepaliveChan
}
