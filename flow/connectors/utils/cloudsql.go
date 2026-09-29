package utils

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"cloud.google.com/go/auth"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// GCPCloudSQLLoginScope authorizes IAM database logins to Cloud SQL.
const GCPCloudSQLLoginScope = "https://www.googleapis.com/auth/sqlservice.login"

type CloudSQLAuth struct {
	// TokenProvider mints Cloud SQL IAM login tokens.
	// When nil, deployment workload identity credentials are created on first use.
	TokenProvider auth.TokenProvider
	lock          sync.Mutex
}

type CloudSQLConnectionConfig struct {
	Host                 string
	RootCa               *string
	TlsHost              string
	DisableTls           bool
	SkipCertVerification bool
}

// VerifyAuthConfig checks that the connection settings are safe for sending an IAM token as the password,
// see common.CloudSQLIAMTLSConfig.Verify.
func (c *CloudSQLAuth) VerifyAuthConfig(connConfig CloudSQLConnectionConfig) error {
	rootCa := ""
	if connConfig.RootCa != nil {
		rootCa = *connConfig.RootCa
	}
	// the connection string is built from the sanitized host, so verify the same value:
	// pasted junk such as "10.0.0.5/db" must not turn an IP into a "DNS name" that skips the TLS host check
	if err := (common.CloudSQLIAMTLSConfig{
		Host:                 internal.SanitizePGHost(connConfig.Host),
		TlsHost:              connConfig.TlsHost,
		RootCa:               rootCa,
		DisableTls:           connConfig.DisableTls,
		SkipCertVerification: connConfig.SkipCertVerification,
	}).Verify(); err != nil {
		return exceptions.NewCloudSQLIAMAuthError(err)
	}
	return nil
}

func GetCloudSQLToken(ctx context.Context, cloudSQLAuth *CloudSQLAuth, connectorName string) (string, error) {
	tokenProvider, err := cloudSQLAuth.getTokenProvider(ctx)
	if err != nil {
		return "", exceptions.NewCloudSQLIAMAuthError(fmt.Errorf("failed to create credentials: %w", err))
	}
	internal.LoggerFromCtx(ctx).Info("Getting Cloud SQL IAM token for connector", slog.String("connector", connectorName))
	// the token provider caches tokens and refreshes them ahead of expiry
	token, err := tokenProvider.Token(ctx)
	if err != nil {
		return "", exceptions.NewCloudSQLIAMAuthError(fmt.Errorf("failed to get token: %w", err))
	}
	if token == nil || token.Value == "" {
		return "", exceptions.NewCloudSQLIAMAuthError(errors.New("token is empty"))
	}
	return token.Value, nil
}

func (c *CloudSQLAuth) getTokenProvider(ctx context.Context) (auth.TokenProvider, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.TokenProvider == nil {
		credentials, err := NewGCPWorkloadIdentityCredentials(ctx, []string{GCPCloudSQLLoginScope})
		if err != nil {
			return nil, err
		}
		c.TokenProvider = credentials
	}
	return c.TokenProvider, nil
}
