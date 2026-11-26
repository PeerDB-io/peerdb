package testutil

import (
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// CreateMySQLTestDB drops and recreates dbName, and drops it again on cleanup.
func CreateMySQLTestDB(t *testing.T, conn *client.Conn, dbName string) {
	t.Helper()
	quoted := "`" + dbName + "`"
	_, err := conn.Execute("DROP DATABASE IF EXISTS " + quoted)
	require.NoError(t, err)
	_, err = conn.Execute("CREATE DATABASE " + quoted)
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := conn.Execute("DROP DATABASE IF EXISTS " + quoted); err != nil {
			t.Logf("drop test db %s failed: %v", dbName, err)
		}
	})
}

// MySQLTestDBName returns mnemonic with a random lowercase suffix.
func MySQLTestDBName(mnemonic string) string {
	return mnemonic + "_" + strings.ToLower(common.RandomString(8))
}
