//go:build tilt

package connmysql

import (
	"os"
	"testing"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Run this smoke test with application default credentials. The ordinary CI
// database is not Cloud SQL.
func TestCloudSQLIAMAuthConnectForMySQL(t *testing.T) {
	host := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_HOST_MYSQL")
	if host == "" {
		t.Skip("FLOW_TESTS_CLOUDSQL_IAM_AUTH_HOST_MYSQL is not set")
	}
	user := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_USERNAME_MYSQL")
	require.NotEmpty(t, user, "missing Cloud SQL MySQL IAM database username")
	// optional, the IAM user may have no grants on any database and still log in
	database := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_DATABASE_MYSQL")
	// Log in with application default credentials.
	dialer, err := cloudsqlconn.NewDialer(t.Context(), cloudsqlconn.WithIAMAuthN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dialer.Close()) })
	utils.UseCloudSQLDialer(dialer)
	t.Cleanup(func() { utils.UseCloudSQLDialer(nil) })
	connector, err := NewMySqlConnector(t.Context(), &protos.MySqlConfig{
		Host:     host, // instance connection name
		User:     user, // service account email without @project.iam.gserviceaccount.com
		Database: database,
		Flavor:   protos.MySqlFlavor_MYSQL_MYSQL,
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connector.Close()) })
	// Ping would only check the catalog connection, run a query on the Cloud SQL connection itself
	require.NoError(t, connector.ConnectionActive(t.Context()))
	rs, err := connector.Execute(t.Context(), "SELECT CURRENT_USER()")
	require.NoError(t, err)
	currentUser, err := rs.GetString(0, 0)
	require.NoError(t, err)
	require.Contains(t, currentUser, user)
}
