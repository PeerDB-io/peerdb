package utils

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"cloud.google.com/go/auth"

	"github.com/PeerDB-io/peerdb/flow/internal"
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
	RootCa               *string
	TlsHost              string
	DisableTls           bool
	SkipCertVerification bool
}

// VerifyAuthConfig checks that the connection settings are safe for sending an IAM token as the password.
// TlsHost must be the instance DNS name from its server certificate: without hostname verification any
// certificate from a shared Cloud SQL CA would be accepted, handing that server a reusable login token.
func (c *CloudSQLAuth) VerifyAuthConfig(connConfig CloudSQLConnectionConfig) error {
	if connConfig.DisableTls {
		return exceptions.NewCloudSQLIAMAuthError(errors.New("TLS is required"))
	}
	if connConfig.SkipCertVerification {
		return exceptions.NewCloudSQLIAMAuthError(errors.New("certificate verification cannot be skipped"))
	}
	if strings.TrimSpace(connConfig.TlsHost) == "" {
		return exceptions.NewCloudSQLIAMAuthError(errors.New("TLS host must be set to the instance DNS name"))
	}
	if connConfig.RootCa == nil || strings.TrimSpace(*connConfig.RootCa) == "" {
		return exceptions.NewCloudSQLIAMAuthError(errors.New("root CA is required"))
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
