//go:build tilt

package connpostgres

import (
	"context"
	"os"
	"strconv"
	"testing"

	"cloud.google.com/go/auth/credentials"
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
	tlsHost := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_TLS_HOST_POSTGRES")
	require.NotEmpty(t, tlsHost, "missing Cloud SQL instance DNS name")
	rootCAFile := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_ROOT_CA_FILE_POSTGRES")
	require.NotEmpty(t, rootCAFile, "missing Cloud SQL PostgreSQL root CA file")
	// #nosec G703 -- test-only CA path supplied by the trusted integration-test environment.
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
		TlsHost:  tlsHost,
		RootCa:   &ca,
		AuthType: protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
	}
	// Log in with application default credentials.
	adc, err := credentials.DetectDefault(&credentials.DetectOptions{Scopes: []string{utils.GCPCloudSQLLoginScope}})
	require.NoError(t, err)
	connConfig, err := ParseConfig(internal.GetPGConnectionString(config, ""), config)
	require.NoError(t, err)
	conn, err := NewPostgresConnFromConfig(t.Context(), connConfig, config.TlsHost, nil,
		&utils.CloudSQLAuth{TokenProvider: adc}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
}
