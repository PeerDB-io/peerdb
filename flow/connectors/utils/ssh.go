package utils

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type SSHTunnel struct {
	*ssh.Client
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
) (SSHTunnel, error) {
	if sshConfig != nil {
		logger := internal.LoggerFromCtx(ctx)
		sshServer := fmt.Sprintf("%s:%d", sshConfig.Host, sshConfig.Port)
		clientConfig, err := GetSSHClientConfig(sshConfig)
		if err != nil {
			logger.Error("Failed to get SSH client config", "error", err)
			return SSHTunnel{}, err
		}

		logger.Info("Setting up SSH connection ", slog.String("Server", sshServer))
		client, err := ssh.Dial("tcp", sshServer, clientConfig)
		if err != nil {
			return SSHTunnel{}, exceptions.NewSSHTunnelSetupError(err)
		}

		return SSHTunnel{Client: client}, nil
	}

	return SSHTunnel{}, nil
}

func (tunnel SSHTunnel) Close() error {
	if tunnel.Client != nil {
		return tunnel.Client.Close()
	}
	return nil
}
