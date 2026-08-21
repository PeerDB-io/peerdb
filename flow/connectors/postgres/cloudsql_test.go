package connpostgres

import (
	"context"
	"testing"

	"cloud.google.com/go/auth"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestPostgresCloudSQLAuthModeAndTLSValidation(t *testing.T) {
	for name, authType := range map[string]protos.PostgresAuthType{
		"password": protos.PostgresAuthType_POSTGRES_PASSWORD,
		"AWS IAM":  protos.PostgresAuthType_POSTGRES_IAM_AUTH,
	} {
		t.Run(name+" remains unchanged", func(t *testing.T) {
			provider, err := newPostgresCloudSQLTokenProviderWithFactory(
				t.Context(),
				&protos.PostgresConfig{AuthType: authType},
				unexpectedPostgresCredentialsFactory,
			)
			require.NoError(t, err)
			require.Nil(t, provider)
		})
	}

	disableTLS := true
	provider, err := newPostgresCloudSQLTokenProviderWithFactory(
		t.Context(),
		&protos.PostgresConfig{
			AuthType:   protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
			DisableTls: &disableTLS,
		},
		unexpectedPostgresCredentialsFactory,
	)
	require.Nil(t, provider)
	require.ErrorContains(t, err, "requires TLS")
	_, err = NewPostgresConnector(t.Context(), nil, &protos.PostgresConfig{
		AuthType:   protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
		DisableTls: &disableTLS,
	})
	require.ErrorContains(t, err, "requires TLS")

	provider, err = newPostgresCloudSQLTokenProviderWithFactory(
		t.Context(),
		&protos.PostgresConfig{
			AuthType:             protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
			SkipCertVerification: true,
		},
		unexpectedPostgresCredentialsFactory,
	)
	require.Nil(t, provider)
	require.ErrorContains(t, err, "requires certificate verification")
	_, err = NewPostgresConnector(t.Context(), nil, &protos.PostgresConfig{
		AuthType:             protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
		SkipCertVerification: true,
	})
	require.ErrorContains(t, err, "requires certificate verification")

	config := &protos.PostgresConfig{
		User:     "configured-db-user",
		AuthType: protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
	}
	var scopes []string
	provider, err = newPostgresCloudSQLTokenProviderWithFactory(
		t.Context(),
		config,
		func(_ context.Context, requestedScopes []string) (auth.TokenProvider, error) {
			scopes = requestedScopes
			return &sequenceTokenProvider{values: []string{"token"}}, nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, provider)
	require.Equal(t, []string{utils.GCPCloudSQLLoginScope}, scopes)
	require.Equal(t, "configured-db-user", config.User)
}

func TestPostgresCloudSQLEmptyTokenRejected(t *testing.T) {
	for _, test := range []struct {
		name  string
		token *auth.Token
	}{
		{name: "nil token"},
		{name: "empty token", token: &auth.Token{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := pgx.ParseConfig("postgres://configured-db-user:persisted-password@localhost/database")
			require.NoError(t, err)

			prepared, err := preparePostgresConnConfig(
				t.Context(),
				config,
				"",
				nil,
				tokenResultProvider{token: test.token},
			)
			require.Nil(t, prepared)
			require.ErrorContains(t, err, "token is empty")
			require.Equal(t, "configured-db-user", config.User)
			require.Equal(t, "persisted-password", config.Password)
		})
	}
}

func TestPostgresCloudSQLTokenAppliedToEveryConnectionConfig(t *testing.T) {
	provider := &sequenceTokenProvider{values: []string{"initial-token", "replication-token"}}
	initial := &pgx.ConnConfig{Config: pgx.ConnConfig{}.Config}
	initial.User = "configured-db-user"
	initial.Password = "persisted-password"
	replication := initial.Copy()
	replication.RuntimeParams = map[string]string{"replication": "database"}

	initialWithToken, err := preparePostgresConnConfig(t.Context(), initial, "", nil, provider)
	require.NoError(t, err)
	replicationWithToken, err := preparePostgresConnConfig(t.Context(), replication, "", nil, provider)
	require.NoError(t, err)

	require.Equal(t, "configured-db-user", initialWithToken.User)
	require.Equal(t, "initial-token", initialWithToken.Password)
	require.Equal(t, "configured-db-user", replicationWithToken.User)
	require.Equal(t, "replication-token", replicationWithToken.Password)
	require.Equal(t, 2, provider.calls)
	require.Equal(t, "persisted-password", initial.Password)
}

func TestPostgresPasswordConfigRemainsUnchanged(t *testing.T) {
	config, err := pgx.ParseConfig("postgres://configured-db-user:password@localhost/database")
	require.NoError(t, err)

	prepared, err := preparePostgresConnConfig(t.Context(), config, "", nil, nil)
	require.NoError(t, err)
	require.Same(t, config, prepared)
	require.Equal(t, "configured-db-user", prepared.User)
	require.Equal(t, "password", prepared.Password)
}

type sequenceTokenProvider struct {
	values []string
	calls  int
}

func (provider *sequenceTokenProvider) Token(context.Context) (*auth.Token, error) {
	value := provider.values[provider.calls]
	provider.calls++
	return &auth.Token{Value: value}, nil
}

type tokenResultProvider struct {
	token *auth.Token
}

func (provider tokenResultProvider) Token(context.Context) (*auth.Token, error) {
	return provider.token, nil
}

func unexpectedPostgresCredentialsFactory(context.Context, []string) (auth.TokenProvider, error) {
	panic("credentials factory should not be called")
}
