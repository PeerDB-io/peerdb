//go:build tilt

package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestAwsRDSIAMAuthConnectForMYSQL(t *testing.T) {
	internal.SkipRDSIAMAuthIfRequested(t)
	internal.SetupRDSIAMAuthAWSCredentials(t)
	conn := internal.RDSIAMAuthMySQLTestConnectionInfo(t)
	mysqlConnector, err := NewMySqlConnector(t.Context(),
		&protos.MySqlConfig{
			Host:       conn.Host,
			User:       conn.Username,
			Port:       3306,
			AuthType:   protos.MySqlAuthType_MYSQL_IAM_AUTH,
			DisableTls: false, // Assumed that AWS Root CA is installed
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(t),
		})
	require.NoError(t, err)
	require.NoError(t, mysqlConnector.ConnectionActive(t.Context()))
}

func TestAwsRDSIAMAuthConnectForMYSQLViaProxy(t *testing.T) {
	internal.SkipRDSIAMAuthIfRequested(t)
	internal.SetupRDSIAMAuthAWSCredentials(t)
	conn := internal.RDSIAMAuthMySQLTestConnectionInfo(t)
	mysqlConnector, err := NewMySqlConnector(t.Context(),
		&protos.MySqlConfig{
			Host:       conn.ProxyHost,
			User:       conn.Username,
			Port:       3306,
			TlsHost:    conn.Host,
			AuthType:   protos.MySqlAuthType_MYSQL_IAM_AUTH,
			DisableTls: false, // Assumed that AWS Root CA is installed
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(t),
		})
	require.NoError(t, err)
	require.NoError(t, mysqlConnector.ConnectionActive(t.Context()))
}
