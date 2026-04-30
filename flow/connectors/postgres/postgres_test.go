package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestNewPostgresConnectorAuthError(t *testing.T) {
	t.Parallel()
	config := internal.GetCatalogPostgresConfigFromEnv(t.Context())
	config.Password = "wrong_password"
	_, err := NewPostgresConnector(t.Context(), internal.NewSettings(nil), config)
	require.Error(t, err)

	var authErr *exceptions.AuthError
	require.ErrorAs(t, err, &authErr)
}
