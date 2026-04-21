package connpostgres

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestAwsRDSIAMAuthConnectForPostgres(t *testing.T) {
	t.Skip("flaky")
	internal.SetupRDSIAMAuthAWSCredentials(t)
	host := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES")
	username := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_POSTGRES")
	postgresConnector, err := NewPostgresConnector(t.Context(),
		nil,
		&protos.PostgresConfig{
			Host:       host,
			Database:   "postgres",
			User:       username,
			Port:       5432,
			AuthType:   protos.PostgresAuthType_POSTGRES_IAM_AUTH,
			RequireTls: true, // Assumed that AWS Root CA is installed
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(),
		})
	require.NoError(t, err)
	defer postgresConnector.Close()
	rows, err := postgresConnector.Conn().Query(t.Context(), "SELECT 1")
	require.NoError(t, err)
	rowCount := 0
	for rows.Next() {
		rowCount++
		var val int
		require.NoError(t, rows.Scan(&val))
		require.Equal(t, 1, val)
	}
	require.Equal(t, 1, rowCount)
}

func TestAwsRDSIAMAuthConnectForPostgresViaProxy(t *testing.T) {
	t.Skip("flaky")
	internal.SetupRDSIAMAuthAWSCredentials(t)
	rdsHost := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES")
	proxyHost := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES_PROXY")

	username := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_POSTGRES")
	postgresConnector, err := NewPostgresConnector(t.Context(),
		nil,
		&protos.PostgresConfig{
			Host:       proxyHost,
			Port:       5432,
			User:       username,
			Database:   "postgres",
			TlsHost:    rdsHost,
			RequireTls: true, // Assumed that AWS Root CA is installed
			AuthType:   protos.PostgresAuthType_POSTGRES_IAM_AUTH,
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(),
		})
	require.NoError(t, err)
	defer postgresConnector.Close()
	rows, err := postgresConnector.Conn().Query(t.Context(), "SELECT 1")
	require.NoError(t, err)
	rowCount := 0
	for rows.Next() {
		rowCount++
		var val int
		require.NoError(t, rows.Scan(&val))
		require.Equal(t, 1, val)
	}
	require.Equal(t, 1, rowCount)
}
