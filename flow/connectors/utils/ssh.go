package utils

import (
	"encoding/base64"
	"fmt"

	"golang.org/x/crypto/ssh"
)

// getSSHClientConfig returns an *ssh.ClientConfig based on provided credentials.
// Parameters:
//
//	user: SSH username
//	password: SSH password (can be empty if using a private key)
//	privateKeyString: Private key as a string (can be empty if using a password)
func GetSSHClientConfig(user, password, privateKeyString string) (*ssh.ClientConfig, error) {
	var authMethods []ssh.AuthMethod

	// Password-based authentication
	if password != "" {
		authMethods = append(authMethods, ssh.Password(password))
	}

	// Private key-based authentication
	if privateKeyString != "" {
		pkey, err := base64.StdEncoding.DecodeString(privateKeyString)
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

	return &ssh.ClientConfig{
		User: user,
		Auth: authMethods,
		//nolint:gosec
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}, nil
}
