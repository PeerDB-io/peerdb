package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestAwsRDSIAMAuthConnectForPostgres(t *testing.T) {
	t.Skip("flaky")
	internal.SetupRDSIAMAuthAWSCredentials(t)
	conn := internal.RDSIAMAuthPostgresTestConnectionInfo(t)
	postgresConnector, err := NewPostgresConnector(t.Context(),
		&protos.PostgresConfig{
			Host:       conn.Host,
			Database:   "postgres",
			User:       conn.Username,
			Port:       5432,
			AuthType:   protos.PostgresAuthType_POSTGRES_IAM_AUTH,
			RequireTls: true, // Assumed that AWS Root CA is installed
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(t),
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
	conn := internal.RDSIAMAuthPostgresTestConnectionInfo(t)
	postgresConnector, err := NewPostgresConnector(t.Context(),
		&protos.PostgresConfig{
			Host:       conn.ProxyHost,
			Port:       5432,
			User:       conn.Username,
			Database:   "postgres",
			TlsHost:    conn.Host,
			RequireTls: true, // Assumed that AWS Root CA is installed
			AuthType:   protos.PostgresAuthType_POSTGRES_IAM_AUTH,
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(t),
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
