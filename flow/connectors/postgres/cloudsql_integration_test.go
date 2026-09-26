package connpostgres

import (
	"context"
	"os"
	"strconv"
	"testing"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Run this smoke test with GKE workload identity or local application default
// credentials. The ordinary CI database is not Cloud SQL.
func TestCloudSQLIAMAuthConnectForPostgres(t *testing.T) {
	host := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_HOST_POSTGRES")
	if host == "" {
		t.Skip("FLOW_TESTS_CLOUDSQL_IAM_AUTH_HOST_POSTGRES is not set")
	}
	user := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_USERNAME_POSTGRES")
	require.NotEmpty(t, user, "missing Cloud SQL PostgreSQL IAM database username")
	rootCAFile := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_ROOT_CA_FILE_POSTGRES")
	require.NotEmpty(t, rootCAFile, "missing Cloud SQL PostgreSQL root CA file")
	rootCA, err := os.ReadFile(rootCAFile)
	require.NoError(t, err)

	port := uint32(5432)
	if value := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_PORT_POSTGRES"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 16)
		require.NoError(t, err)
		port = uint32(parsed)
	}
	database := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_DATABASE_POSTGRES")
	if database == "" {
		database = "postgres"
	}
	ca := string(rootCA)
	config := &protos.PostgresConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Database: database,
		RootCa:   &ca,
		AuthType: protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
	}
	env := map[string]string{"PEERDB_CDC_STORE_ENABLED": "false"}
	var connector *PostgresConnector
	if os.Getenv("PEERDB_GCP_WORKLOAD_IDENTITY_TOKEN_FILE") != "" {
		connector, err = NewPostgresConnector(t.Context(), env, config)
	} else {
		connector, err = newPostgresConnectorWithCloudSQLTokenProvider(
			t.Context(), env, config, protos.DBType_DBTYPE_UNKNOWN,
			func(ctx context.Context, config *protos.PostgresConfig) (auth.TokenProvider, error) {
				return newPostgresCloudSQLTokenProviderWithFactory(ctx, config,
					func(_ context.Context, scopes []string) (auth.TokenProvider, error) {
						return credentials.DetectDefault(&credentials.DetectOptions{Scopes: scopes})
					})
			},
		)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connector.Close()) })

	var one int
	require.NoError(t, connector.Conn().QueryRow(t.Context(), "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)
}
