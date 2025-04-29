package connpostgres

import (
	"os"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestAwsRDSIAMAuthConnectForPostgres(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_ACCESS_KEY_ID"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SECRET_ACCESS_KEY"))
	t.Setenv("AWS_SESSION_TOKEN", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SESSION_TOKEN"))
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
	defer postgresConnector.Close()
	rows, err := postgresConnector.Conn().Query(t.Context(), "SELECT 1")
	require.NoError(t, err)
	rowCount := 0
	for rows.Next() {
		rowCount++
		var val int
		err = rows.Scan(&val)
		require.NoError(t, err)
		require.Equal(t, 1, val)
	}
	require.Equal(t, 1, rowCount)
}

func TestAwsRDSIAMAuthConnectForPostgresViaProxy(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_ACCESS_KEY_ID"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SECRET_ACCESS_KEY"))
	t.Setenv("AWS_SESSION_TOKEN", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SESSION_TOKEN"))
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
	defer postgresConnector.Close()
	rows, err := postgresConnector.Conn().Query(t.Context(), "SELECT 1")
	require.NoError(t, err)
	rowCount := 0
	for rows.Next() {
		rowCount++
		var val int
		err = rows.Scan(&val)
		require.NoError(t, err)
		require.Equal(t, 1, val)
	}
	require.Equal(t, 1, rowCount)
}
