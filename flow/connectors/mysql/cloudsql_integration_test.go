package connmysql

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Run this smoke test where the PeerDB GKE workload identity token file and
// Cloud SQL instance are available. The ordinary CI database is not Cloud SQL.
func TestCloudSQLIAMAuthConnectForMySQL(t *testing.T) {
	host := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_HOST_MYSQL")
	if host == "" {
		t.Skip("FLOW_TESTS_CLOUDSQL_IAM_AUTH_HOST_MYSQL is not set")
	}
	user := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_USERNAME_MYSQL")
	require.NotEmpty(t, user, "missing Cloud SQL MySQL IAM database username")
	rootCAFile := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_ROOT_CA_FILE_MYSQL")
	require.NotEmpty(t, rootCAFile, "missing Cloud SQL MySQL root CA file")
	rootCA, err := os.ReadFile(rootCAFile)
	require.NoError(t, err)

	port := uint32(3306)
	if value := os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_PORT_MYSQL"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 16)
		require.NoError(t, err)
		port = uint32(parsed)
	}
	ca := string(rootCA)
	connector, err := NewMySqlConnector(t.Context(), &protos.MySqlConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Database: os.Getenv("FLOW_TESTS_CLOUDSQL_IAM_AUTH_DATABASE_MYSQL"),
		RootCa:   &ca,
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
		Flavor:   protos.MySqlFlavor_MYSQL_MYSQL,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connector.Close()) })

	require.NoError(t, connector.ConnectionActive(t.Context()))
}
