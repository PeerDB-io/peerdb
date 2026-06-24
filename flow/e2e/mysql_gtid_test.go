package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestMySQLGetMasterGTIDSet(t *testing.T) {
	suffix := "mygtid_" + strings.ToLower(common.RandomString(8))
	source, err := SetupMySQL(t, suffix)
	require.NoError(t, err)
	t.Cleanup(func() { source.Teardown(t, t.Context(), suffix) })

	ctx := t.Context()

	gtidEnabled := source.Config.ReplicationMechanism == protos.MySqlReplicationMechanism_MYSQL_GTID

	if !gtidEnabled {
		_, err := source.GetMasterGTIDSet(ctx)
		require.ErrorContains(t, err, "gtid mode is not enabled")
		return
	}

	// commit a transaction
	table := fmt.Sprintf("e2e_test_%s.gtidcheck", suffix)
	require.NoError(t, source.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", table)))
	require.NoError(t, source.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table)))

	// confirm that the transaction left a non-empty GTID set
	gset, err := source.GetMasterGTIDSet(ctx)
	require.NoError(t, err)
	require.NotNil(t, gset)
	require.False(t, gset.IsEmpty(), "a committed transaction should leave a non-empty GTID set")

	// confirm the tested flavor generates the expected concrete GTID set type
	if source.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		require.IsType(t, &mysql.MariadbGTIDSet{}, gset)
	} else {
		require.IsType(t, &mysql.MysqlGTIDSet{}, gset)
	}

	// confirm that the GTID set round-trips through the flavor-aware parser
	reparsed, err := mysql.ParseGTIDSet(source.Flavor(), gset.String())
	require.NoError(t, err)
	require.True(t, reparsed.Equal(gset), "re-parsed GTID set %q should equal the original %q", reparsed.String(), gset.String())
}
