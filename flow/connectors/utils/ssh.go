package utils

import (
	"encoding/base64"
	"errors"
	"fmt"

	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

// getSSHClientConfig returns an *ssh.ClientConfig based on provided credentials.
// Parameters:
//
//	user: SSH username
//	password: SSH password (can be empty if using a private key)
//	privateKeyString: Private key as a string (can be empty if using a password)
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
