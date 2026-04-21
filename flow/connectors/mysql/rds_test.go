package connmysql

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestAwsRDSIAMAuthConnectForMYSQL(t *testing.T) {
	internal.SetupRDSIAMAuthAWSCredentials(t)
	host := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL")
	username := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_MYSQL")
	mysqlConnector, err := NewMySqlConnector(t.Context(),
		&protos.MySqlConfig{
			Host:       host,
			Database:   "postgres",
			User:       username,
			Port:       5432,
			AuthType:   protos.MySqlAuthType_MYSQL_IAM_AUTH,
			DisableTls: false, // Assumed that AWS Root CA is installed
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(),
		})
	require.NoError(t, err)
	require.NoError(t, mysqlConnector.Ping(t.Context()))
}

func TestAwsRDSIAMAuthConnectForMYSQLViaProxy(t *testing.T) {
	internal.SetupRDSIAMAuthAWSCredentials(t)
	rdsHost := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL")
	proxyHost := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL_PROXY")

	username := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_MYSQL")
	mysqlConnector, err := NewMySqlConnector(t.Context(),
		&protos.MySqlConfig{
			Host:       proxyHost,
			Database:   "postgres",
			User:       username,
			Port:       5432,
			TlsHost:    rdsHost,
			AuthType:   protos.MySqlAuthType_MYSQL_IAM_AUTH,
			DisableTls: false, // Assumed that AWS Root CA is installed
			AwsAuth:    internal.RDSIAMAuthAssumeRoleConfig(),
		})
	require.NoError(t, err)
	require.NoError(t, mysqlConnector.Ping(t.Context()))
}
