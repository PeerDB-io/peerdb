//go:build tilt

package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestNewPostgresConnectorAuthError(t *testing.T) {
	t.Parallel()
	config := internal.GetCatalogPostgresConfigFromEnv(t.Context())
	config.Password = "wrong_password"
	_, err := NewPostgresConnector(t.Context(), nil, config)
	require.Error(t, err)

	var authErr *exceptions.AuthError
	require.ErrorAs(t, err, &authErr)
}

func TestCloudSQLPostgresDerivedUserIsInConnectionString(t *testing.T) {
	t.Setenv("PEERDB_GCP_WORKLOAD_IDENTITY_TARGET_SERVICE_ACCOUNT",
		"test-sa@project.iam.gserviceaccount.com")
	original := &protos.PostgresConfig{
		Host:     "project:region:instance",
		Database: "postgres",
		AuthType: protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
	}

	config, dialer, err := prepareCloudSQLPostgresConfig(original)
	require.NoError(t, err)
	require.NotNil(t, dialer)
	require.Equal(t, "test-sa@project.iam", config.User)
	require.Empty(t, original.User)

	connString := internal.GetPGConnectionString(config, "")
	connConfig, err := ParseConfig(connString, config)
	require.NoError(t, err)
	require.Equal(t, config.User, connConfig.User)
}
