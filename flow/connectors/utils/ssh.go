package utils

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/sdk/log"
	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

const (
	SSHKeepaliveInterval = 15 * time.Second
	SSHRequestTimeout    = 10 * time.Second
)

type SSHTunnel struct {
	*ssh.Client
	logger    log.Logger
	watch     chan struct{}
	err       error
	closeOnce sync.Once
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
		return nil, fmt.Errorf("no authentication methods provided")
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
	if sshConfig == nil {
		return nil, nil
	}

	logger := internal.LoggerFromCtx(ctx)
	sshServer := shared.JoinHostPort(sshConfig.Host, sshConfig.Port)
	clientConfig, err := GetSSHClientConfig(sshConfig)
	if err != nil {
		logger.Error("Failed to get SSH client config", slog.Any("error", err))
		return nil, err
	}

	logger.Info("Setting up SSH connection", slog.String("Server", sshServer))
	client, err := ssh.Dial("tcp", sshServer, clientConfig)
	if err != nil {
		return nil, exceptions.NewSSHTunnelSetupError(err)
	}

	tunnel := &SSHTunnel{
		Client: client,
		logger: internal.SlogLoggerFromCtx(ctx),
		watch:  make(chan struct{}),
	}

	go tunnel.runKeepaliveLoop()

	return tunnel, nil
}

func (tunnel *SSHTunnel) Watch() <-chan struct{} {
	if tunnel == nil || tunnel.Client == nil {
		return nil
	}
	return tunnel.watch
}

func (tunnel *SSHTunnel) Close() error {
	if tunnel == nil || tunnel.Client == nil {
		return nil
	}
	tunnel.shutdown(nil)
	return tunnel.err
}

func (tunnel *SSHTunnel) shutdown(err error) {
	tunnel.closeOnce.Do(func() {
		tunnel.err = err
		close(tunnel.watch)
		if closeErr := tunnel.Client.Close(); closeErr != nil && tunnel.err == nil {
			tunnel.err = closeErr
		}
	})
}

func (tunnel *SSHTunnel) runKeepaliveLoop() {
	ticker := time.NewTicker(SSHKeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-tunnel.watch:
			// tunnel closed
			return
		case <-ticker.C:
			success := tunnel.sendKeepAlive()
			if !success {
				return
			}
		}
	}
}

func (tunnel *SSHTunnel) sendKeepAlive() bool {
	sent := make(chan error, 1)
	go func() {
		_, _, err := tunnel.SendRequest("keepalive@openssh.com", true, nil)
		sent <- err
	}()
	timer := time.NewTimer(SSHRequestTimeout)
	defer timer.Stop()
	select {
	case err := <-sent:
		if err != nil {
			tunnel.logger.Error("SSH keepalive failed, tearing down tunnel", slog.Any("error", err))
			tunnel.shutdown(err)
			return false
		}
		return true
	case <-timer.C:
		tunnel.logger.Error("SSH keepalive request timed out, tearing down tunnel")
		tunnel.shutdown(errors.New("SSH keepalive request timed out"))
		return false
	case <-tunnel.watch:
		return false
	}
}
