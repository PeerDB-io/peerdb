package utils

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/auth"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestCloudSQLAuthVerifyAuthConfig(t *testing.T) {
	rootCA := "root-ca"
	emptyRootCA := " "
	for _, test := range []struct {
		name       string
		connConfig CloudSQLConnectionConfig
		valid      bool
	}{
		{name: "root CA", connConfig: CloudSQLConnectionConfig{RootCa: &rootCA}, valid: true},
		{name: "TLS host", connConfig: CloudSQLConnectionConfig{TlsHost: "cloudsql.google.internal"}, valid: true},
		{name: "TLS disabled", connConfig: CloudSQLConnectionConfig{RootCa: &rootCA, DisableTls: true}},
		{name: "skip cert verification", connConfig: CloudSQLConnectionConfig{RootCa: &rootCA, SkipCertVerification: true}},
		{name: "no root CA or TLS host", connConfig: CloudSQLConnectionConfig{}},
		{name: "empty root CA", connConfig: CloudSQLConnectionConfig{RootCa: &emptyRootCA}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := (&CloudSQLAuth{}).VerifyAuthConfig(test.connConfig)
			if test.valid {
				require.NoError(t, err)
			} else {
				_, ok := errors.AsType[*exceptions.CloudSQLIAMAuthError](err)
				require.True(t, ok, "expected CloudSQLIAMAuthError, got %v", err)
			}
		})
	}
}

func TestGetCloudSQLToken(t *testing.T) {
	token, err := GetCloudSQLToken(t.Context(), &CloudSQLAuth{
		TokenProvider: staticTokenProvider{token: &auth.Token{Value: "token"}},
	}, "TEST")
	require.NoError(t, err)
	require.Equal(t, "token", token)

	for _, test := range []struct {
		name     string
		provider staticTokenProvider
	}{
		{name: "provider error", provider: staticTokenProvider{err: errors.New("boom")}},
		{name: "nil token"},
		{name: "empty token", provider: staticTokenProvider{token: &auth.Token{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := GetCloudSQLToken(t.Context(), &CloudSQLAuth{TokenProvider: test.provider}, "TEST")
			_, ok := errors.AsType[*exceptions.CloudSQLIAMAuthError](err)
			require.True(t, ok, "expected CloudSQLIAMAuthError, got %v", err)
		})
	}
}

type staticTokenProvider struct {
	token *auth.Token
	err   error
}

func (provider staticTokenProvider) Token(context.Context) (*auth.Token, error) {
	return provider.token, provider.err
}
