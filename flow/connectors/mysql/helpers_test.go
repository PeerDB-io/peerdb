package connmysql

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
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

func createTestDB(t *testing.T, ctx context.Context, c *MySqlConnector, dbName string) {
	t.Helper()
	quoted := "`" + dbName + "`"
	_, err := c.Execute(ctx, "DROP DATABASE IF EXISTS "+quoted)
	require.NoError(t, err)
	_, err = c.Execute(ctx, "CREATE DATABASE "+quoted)
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := c.Execute(context.Background(), "DROP DATABASE IF EXISTS "+quoted); err != nil {
			t.Logf("drop test db %s failed: %v", dbName, err)
		}
	})
}

func testDBName(mnemonic string) string {
	return mnemonic + "_" + strings.ToLower(common.RandomString(8))
}
