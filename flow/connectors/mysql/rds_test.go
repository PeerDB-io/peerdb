package connmysql

import (
	"os"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestAwsRDSIAMAuthConnectForMYSQL(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_ACCESS_KEY_ID"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SECRET_ACCESS_KEY"))
	t.Setenv("AWS_SESSION_TOKEN", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SESSION_TOKEN"))
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
			AwsAuth: &protos.AwsAuthenticationConfig{
				AuthType: protos.AwsIAMAuthConfigType_IAM_AUTH_ASSUME_ROLE,
				AuthConfig: &protos.AwsAuthenticationConfig_Role{
					Role: &protos.AWSAuthAssumeRoleConfig{
						AssumeRoleArn:  os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_ASSUME_ROLE"),
						ChainedRoleArn: ptr.String(os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_CHAINED_ROLE")),
					},
				},
			},
		})
	require.NoError(t, err)
	err = mysqlConnector.Ping(t.Context())
	require.NoError(t, err)
}

func TestAwsRDSIAMAuthConnectForMYSQLViaProxy(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_ACCESS_KEY_ID"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SECRET_ACCESS_KEY"))
	t.Setenv("AWS_SESSION_TOKEN", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SESSION_TOKEN"))
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
			AwsAuth: &protos.AwsAuthenticationConfig{
				AuthType: protos.AwsIAMAuthConfigType_IAM_AUTH_ASSUME_ROLE,
				AuthConfig: &protos.AwsAuthenticationConfig_Role{
					Role: &protos.AWSAuthAssumeRoleConfig{
						AssumeRoleArn:  os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_ASSUME_ROLE"),
						ChainedRoleArn: ptr.String(os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_CHAINED_ROLE")),
					},
				},
			},
		})
	require.NoError(t, err)
	err = mysqlConnector.Ping(t.Context())
	require.NoError(t, err)
}
