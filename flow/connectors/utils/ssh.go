package utils

import (
	"os"

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
		signer, err := ssh.ParsePrivateKey([]byte(privateKeyString))
		if err != nil {
			return nil, err
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	if len(authMethods) == 0 {
		return nil, os.ErrInvalid // No authentication method provided
	}

	return &ssh.ClientConfig{
		User:            user,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}, nil
}
