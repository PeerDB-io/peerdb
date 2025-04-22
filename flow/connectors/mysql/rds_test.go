package connmysql

import (
	"os"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestAwsRDSIAMAuthConnectForMYSQL(t *testing.T) {
	host := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL")
	username := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_MYSQL")
	mysqlConnector, err := NewMySqlConnector(t.Context(),
		&protos.MySqlConfig{
			Host:       host,
			Database:   "postgres",
			User:       username,
			Port:       5432,
			AuthType:   protos.MySqlAuthType_MYSQL_AUTH_TYPE_IAM_AUTH,
			DisableTls: false, // Assumed that AWS Root CA is installed
			AwsAuth: &protos.AwsAuthenticationConfig{
				AuthConfig: &protos.AwsAuthenticationConfig_Role{
					Role: &protos.AWSAuthAssumeRoleConfig{
						RoleArn:        os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_ASSUME_ROLE"),
						ChainedRoleArn: ptr.String(os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_CHAINED_ROLE")),
					},
				},
			},
		})
	require.NoError(t, err)
	err = mysqlConnector.Ping(t.Context())
	require.NoError(t, err)
}
