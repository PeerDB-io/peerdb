package connmysql

import (
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestMySQLGetMasterGTIDSet(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)

	gtidEnabled := connector.config.ReplicationMechanism == protos.MySqlReplicationMechanism_MYSQL_GTID

	if !gtidEnabled {
		_, err := connector.GetMasterGTIDSet(ctx)
		require.ErrorContains(t, err, "gtid mode is not enabled")
		return
	}

	// commit a transaction
	dbName := testDBName("gtidcheck")
	createTestDB(t, ctx, connector, dbName)
	table := fmt.Sprintf("`%s`.gtidcheck", dbName)
	_, err := connector.Execute(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", table))
	require.NoError(t, err)
	_, err = connector.Execute(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table))
	require.NoError(t, err)

	// confirm that the transaction left a non-empty GTID set
	gset, err := connector.GetMasterGTIDSet(ctx)
	require.NoError(t, err)
	require.NotNil(t, gset)
	require.False(t, gset.IsEmpty(), "a committed transaction should leave a non-empty GTID set")

	// confirm the tested flavor generates the expected concrete GTID set type
	if connector.config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		require.IsType(t, &mysql.MariadbGTIDSet{}, gset)
	} else {
		require.IsType(t, &mysql.MysqlGTIDSet{}, gset)
	}

	// confirm that the GTID set round-trips through the flavor-aware parser
	reparsed, err := mysql.ParseGTIDSet(connector.Flavor(), gset.String())
	require.NoError(t, err)
	require.True(t, reparsed.Equal(gset), "re-parsed GTID set %q should equal the original %q", reparsed.String(), gset.String())
}
