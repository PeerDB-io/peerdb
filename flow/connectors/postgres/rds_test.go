package connpostgres

import (
	"os"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestAwsRDSIAMAuthConnectForPostgres(t *testing.T) {
	host := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES")
	username := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_POSTGRES")
	postgresConnector, err := NewPostgresConnector(t.Context(),
		nil,
		&protos.PostgresConfig{
			Host:       host,
			Database:   "postgres",
			User:       username,
			Port:       5432,
			AuthType:   protos.PostgresAuthType_POSTGRES_AUTH_TYPE_IAM_AUTH,
			RequireTls: true, // Assumed that AWS Root CA is installed
			AwsAuth: &protos.AwsAuthenticationConfig{
				AuthConfig: &protos.AwsAuthenticationConfig_Role{
					Role: &protos.AWSAuthAssumeRoleConfig{
						RoleArn:        os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_ASSUME_ROLE"),
						ChainedRoleArn: ptr.String(os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_CHAINED_ROLE")),
					},
				},
			},
		})
	defer postgresConnector.Close()
	require.NoError(t, err)
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
