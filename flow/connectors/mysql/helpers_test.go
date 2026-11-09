//go:build tilt

package connmysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

func newTestConnector(t *testing.T, ctx context.Context) *MySqlConnector {
	t.Helper()
	flavor, mechanism := internal.MySQLTestFlavorAndMechanism(t)
	config := internal.GetMySQLConfigFromEnv(flavor, mechanism)
	connector, err := NewMySqlConnector(ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := connector.Close(); err != nil {
			t.Logf("connector close failed: %v", err)
		}
	})
	require.NoError(t, ConfigureReplication(t, connector, config))
	return connector
}

func createTestDB(t *testing.T, _ context.Context, c *MySqlConnector, dbName string) {
	t.Helper()
	testutil.CreateMySQLTestDB(t, c.Conn(), dbName)
}

func testDBName(mnemonic string) string {
	return testutil.MySQLTestDBName(mnemonic)
}
