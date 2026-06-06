package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestAwsRDSIAMAuthConnectForMYSQL(t *testing.T) {
	internal.SetupRDSIAMAuthAWSCredentials(t)
	conn := internal.RDSIAMAuthMySQLTestConnectionInfo(t)
	mysqlConnector, err := NewMySqlConnector(t.Context(),
		&protos.MySqlConfig{
			Host:       conn.Host,
			Database:   "postgres",
			User:       conn.Username,
			Port:       5432,
			AuthType:   protos.MySqlAuthType_MYSQL_IAM_AUTH,
			DisableTls: false, // Assumed that AWS Root CA is installed
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(t),
		})
	require.NoError(t, err)
	require.NoError(t, mysqlConnector.ConnectionActive(t.Context()))
}

func TestAwsRDSIAMAuthConnectForMYSQLViaProxy(t *testing.T) {
	internal.SetupRDSIAMAuthAWSCredentials(t)
	conn := internal.RDSIAMAuthMySQLTestConnectionInfo(t)
	mysqlConnector, err := NewMySqlConnector(t.Context(),
		&protos.MySqlConfig{
			Host:       conn.ProxyHost,
			Database:   "postgres",
			User:       conn.Username,
			Port:       5432,
			TlsHost:    conn.Host,
			AuthType:   protos.MySqlAuthType_MYSQL_IAM_AUTH,
			DisableTls: false, // Assumed that AWS Root CA is installed
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(t),
		})
	require.NoError(t, err)
	require.NoError(t, mysqlConnector.ConnectionActive(t.Context()))
}
