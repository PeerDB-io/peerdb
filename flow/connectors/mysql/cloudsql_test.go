package connmysql

import (
	"context"
	"testing"

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

	config := &protos.MySqlConfig{
		User:     "configured-db-user",
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
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
