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
	tlsHost := "1-abc.us-central1.sql.goog"
	for _, test := range []struct {
		name       string
		connConfig CloudSQLConnectionConfig
		valid      bool
	}{
		{name: "TLS host and root CA", connConfig: CloudSQLConnectionConfig{TlsHost: tlsHost, RootCa: &rootCA}, valid: true},
		{name: "root CA only", connConfig: CloudSQLConnectionConfig{RootCa: &rootCA}},
		{name: "blank TLS host", connConfig: CloudSQLConnectionConfig{TlsHost: " ", RootCa: &rootCA}},
		{name: "DNS host without TLS host", connConfig: CloudSQLConnectionConfig{Host: tlsHost, RootCa: &rootCA}, valid: true},
		{name: "IPv4 host without TLS host", connConfig: CloudSQLConnectionConfig{Host: "35.238.144.132", RootCa: &rootCA}},
		{name: "IPv6 host without TLS host", connConfig: CloudSQLConnectionConfig{Host: "[2001:db8::1]", RootCa: &rootCA}},
		{name: "IPv4 host with pasted suffix", connConfig: CloudSQLConnectionConfig{Host: "35.238.144.132/postgres", RootCa: &rootCA}},
		{name: "IPv4 host with query suffix", connConfig: CloudSQLConnectionConfig{Host: " 35.238.144.132?sslmode=require", RootCa: &rootCA}},
		{
			name:       "IP host with TLS host",
			connConfig: CloudSQLConnectionConfig{Host: "35.238.144.132", TlsHost: tlsHost, RootCa: &rootCA},
			valid:      true,
		},
		{
			name:       "DNS host, TLS disabled",
			connConfig: CloudSQLConnectionConfig{Host: tlsHost, RootCa: &rootCA, DisableTls: true},
		},
		{name: "TLS host only", connConfig: CloudSQLConnectionConfig{TlsHost: tlsHost}},
		{name: "empty root CA", connConfig: CloudSQLConnectionConfig{TlsHost: tlsHost, RootCa: &emptyRootCA}},
		{name: "TLS disabled", connConfig: CloudSQLConnectionConfig{TlsHost: tlsHost, RootCa: &rootCA, DisableTls: true}},
		{
			name:       "skip cert verification",
			connConfig: CloudSQLConnectionConfig{TlsHost: tlsHost, RootCa: &rootCA, SkipCertVerification: true},
		},
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
