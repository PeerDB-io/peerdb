//go:build tilt

package connpostgres

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// Run this smoke test with application default credentials. The ordinary CI
// database is not Cloud SQL.
func TestCloudSQLIAMAuthConnectForPostgres(t *testing.T) {
	host := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_HOST_POSTGRES")
	if host == "" {
		t.Skip("FLOW_TESTS_CLOUDSQL_IAM_AUTH_HOST_POSTGRES is not set")
	}
	user := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_USERNAME_POSTGRES")
	require.NotEmpty(t, user, "missing Cloud SQL PostgreSQL IAM database username")
	database := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_DATABASE_POSTGRES")
	if database == "" {
		database = "postgres"
	}
	config := &protos.PostgresConfig{
		Host:     host, // instance connection name
		User:     user,
		Database: database,
		AuthType: protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
	}
	// Log in with application default credentials.
	dialer, err := cloudsqlconn.NewDialer(t.Context(), cloudsqlconn.WithIAMAuthN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dialer.Close()) })
	utils.UseCloudSQLDialer(dialer)
	t.Cleanup(func() { utils.UseCloudSQLDialer(nil) })
	connConfig, err := ParseConfig(internal.GetPGConnectionString(config, ""), config)
	require.NoError(t, err)
	conn, err := NewPostgresConnFromConfig(t.Context(), connConfig, "", nil,
		&utils.CloudSQLDialer{Instance: host}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
}
