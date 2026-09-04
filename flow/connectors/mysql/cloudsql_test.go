package connmysql

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestMySQLCloudSQLAuthModeAndTLSValidation(t *testing.T) {
	for name, authType := range map[string]protos.MySqlAuthType{
		"password": protos.MySqlAuthType_MYSQL_PASSWORD,
		"AWS IAM":  protos.MySqlAuthType_MYSQL_IAM_AUTH,
	} {
		t.Run(name+" remains unchanged", func(t *testing.T) {
			provider, err := newMySQLCloudSQLTokenProviderWithFactory(
				t.Context(),
				&protos.MySqlConfig{AuthType: authType},
				unexpectedMySQLCredentialsFactory,
			)
			require.NoError(t, err)
			require.Nil(t, provider)
		})
	}

	provider, err := newMySQLCloudSQLTokenProviderWithFactory(
		t.Context(),
		&protos.MySqlConfig{
			AuthType:   protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
			DisableTls: true,
		},
		unexpectedMySQLCredentialsFactory,
	)
	require.Nil(t, provider)
	require.ErrorContains(t, err, "requires TLS")
	_, err = NewMySqlConnector(t.Context(), &protos.MySqlConfig{
		AuthType:   protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
		DisableTls: true,
	})
	require.ErrorContains(t, err, "requires TLS")

	provider, err = newMySQLCloudSQLTokenProviderWithFactory(
		t.Context(),
		&protos.MySqlConfig{
			AuthType:             protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
			SkipCertVerification: true,
		},
		unexpectedMySQLCredentialsFactory,
	)
	require.Nil(t, provider)
	require.ErrorContains(t, err, "requires certificate verification")
	_, err = NewMySqlConnector(t.Context(), &protos.MySqlConfig{
		AuthType:             protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
		SkipCertVerification: true,
	})
	require.ErrorContains(t, err, "requires certificate verification")

	provider, err = newMySQLCloudSQLTokenProviderWithFactory(
		t.Context(),
		&protos.MySqlConfig{AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH},
		unexpectedMySQLCredentialsFactory,
	)
	require.Nil(t, provider)
	require.ErrorContains(t, err, "without tls_host requires a non-empty root CA")
	_, err = NewMySqlConnector(t.Context(), &protos.MySqlConfig{
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
	})
	require.ErrorContains(t, err, "without tls_host requires a non-empty root CA")
	emptyRootCA := ""
	provider, err = newMySQLCloudSQLTokenProviderWithFactory(
		t.Context(),
		&protos.MySqlConfig{
			AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
			RootCa:   &emptyRootCA,
		},
		unexpectedMySQLCredentialsFactory,
	)
	require.Nil(t, provider)
	require.ErrorContains(t, err, "requires a non-empty root CA")

	config := &protos.MySqlConfig{
		User:     "configured-db-user",
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
		TlsHost:  "cloudsql.google.internal",
	}
	var scopes []string
	provider, err = newMySQLCloudSQLTokenProviderWithFactory(
		t.Context(),
		config,
		func(_ context.Context, requestedScopes []string) (auth.TokenProvider, error) {
			scopes = requestedScopes
			return &mysqlSequenceTokenProvider{values: []string{"token"}}, nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, provider)
	require.Equal(t, []string{utils.GCPCloudSQLLoginScope}, scopes)
	require.Equal(t, "configured-db-user", config.User)
}

func TestMySQLCloudSQLTLSIdentityPolicy(t *testing.T) {
	_, err := mySQLTLSConfig(&protos.MySqlConfig{
		Host:     "synthetic-rpe-alias.internal",
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
	})
	require.ErrorContains(t, err, "requires a non-empty root CA")

	rootCA := generateMySQLRootCA(t)
	chainOnlyConfig, err := mySQLTLSConfig(&protos.MySqlConfig{
		Host:     "synthetic-rpe-alias.internal",
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
		RootCa:   &rootCA,
	})
	require.NoError(t, err)
	require.True(t, chainOnlyConfig.InsecureSkipVerify)
	require.NotNil(t, chainOnlyConfig.VerifyConnection)
	require.Empty(t, chainOnlyConfig.ServerName)

	tlsHostConfig, err := mySQLTLSConfig(&protos.MySqlConfig{
		Host:     "synthetic-rpe-alias.internal",
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
		TlsHost:  "cloudsql.google.internal",
	})
	require.NoError(t, err)
	require.False(t, tlsHostConfig.InsecureSkipVerify)
	require.Nil(t, tlsHostConfig.VerifyConnection)
	require.Equal(t, "cloudsql.google.internal", tlsHostConfig.ServerName)
}

func TestMySQLCloudSQLEmptyTokenRejected(t *testing.T) {
	for _, test := range []struct {
		name  string
		token *auth.Token
	}{
		{name: "nil token"},
		{name: "empty token", token: &auth.Token{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := &protos.MySqlConfig{
				User:     "configured-db-user",
				Password: "persisted-password",
			}
			connector := &MySqlConnector{
				config:       config,
				cloudSQLAuth: mysqlTokenResultProvider{token: test.token},
			}

			prepared, err := connector.configWithAuthToken(t.Context(), false)
			require.Nil(t, prepared)
			require.ErrorContains(t, err, "token is empty")
			require.Equal(t, "configured-db-user", config.User)
			require.Equal(t, "persisted-password", config.Password)
		})
	}
}

func TestMySQLCloudSQLTokenAppliedToOrdinaryAndBinlogConfigs(t *testing.T) {
	provider := &mysqlSequenceTokenProvider{values: []string{"ordinary-token", "binlog-token"}}
	config := &protos.MySqlConfig{
		Host:       "db.example.com",
		Port:       3306,
		User:       "configured-db-user",
		Password:   "persisted-password",
		Flavor:     protos.MySqlFlavor_MYSQL_MYSQL,
		DisableTls: false,
	}
	connector := &MySqlConnector{
		logger:       internal.LoggerFromCtx(t.Context()),
		config:       config,
		cloudSQLAuth: provider,
	}

	ordinaryConfig, err := connector.configWithAuthToken(t.Context(), false)
	require.NoError(t, err)
	binlogConfig, err := connector.buildBinlogSyncerConfig(t.Context(), map[string]string{
		"PEERDB_MYSQL_EVENT_CACHE_COUNT": "10240",
	})
	require.NoError(t, err)

	require.Equal(t, "configured-db-user", ordinaryConfig.User)
	require.Equal(t, "ordinary-token", ordinaryConfig.Password)
	require.Equal(t, "configured-db-user", binlogConfig.User)
	require.Equal(t, "binlog-token", binlogConfig.Password)
	require.Equal(t, 2, provider.calls)
	require.Equal(t, "persisted-password", config.Password)
}

func TestMySQLPasswordConfigRemainsUnchanged(t *testing.T) {
	config := &protos.MySqlConfig{User: "configured-db-user", Password: "password"}
	connector := &MySqlConnector{config: config}

	prepared, err := connector.configWithAuthToken(t.Context(), false)
	require.NoError(t, err)
	require.Same(t, config, prepared)
	require.Equal(t, "configured-db-user", prepared.User)
	require.Equal(t, "password", prepared.Password)
}

type mysqlSequenceTokenProvider struct {
	values []string
	calls  int
}

func (provider *mysqlSequenceTokenProvider) Token(context.Context) (*auth.Token, error) {
	value := provider.values[provider.calls]
	provider.calls++
	return &auth.Token{Value: value}, nil
}

type mysqlTokenResultProvider struct {
	token *auth.Token
}

func (provider mysqlTokenResultProvider) Token(context.Context) (*auth.Token, error) {
	return provider.token, nil
}

func unexpectedMySQLCredentialsFactory(context.Context, []string) (auth.TokenProvider, error) {
	panic("credentials factory should not be called")
}

func generateMySQLRootCA(t *testing.T) string {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test root"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}))
}
