package connmysql

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func setupMySQLConnectorWithDirectSSH(ctx context.Context, t *testing.T) *MySqlConnector {
	t.Helper()

	sshPort, err := strconv.ParseUint(utils.SSHServerPort, 10, 32)
	require.NoError(t, err)

	mysqlHost, mysqlPort, mysqlRootPass := resolveMySQL(t)
	connector, err := NewMySqlConnector(ctx, &protos.MySqlConfig{
		Host:                 mysqlHost,
		Port:                 mysqlPort,
		User:                 "root",
		Password:             mysqlRootPass,
		Database:             "mysql",
		ReplicationMechanism: protos.MySqlReplicationMechanism_MYSQL_FILEPOS,
		SshConfig: &protos.SSHConfig{
			Host:     "localhost",
			Port:     uint32(sshPort),
			User:     "testuser",
			Password: "testpass",
		},
		DisableTls: true,
	})
	require.NoError(t, err)
	require.NoError(t, connector.ConnectionActive(ctx))

	return connector
}

func TestMySQLSyncerClose(t *testing.T) {
	t.Parallel()
	if internal.MySQLTestVersionIsMaria() {
		t.Skip("Skipping for MariaDB")
	}

	ctx := t.Context()
	connector := setupMySQLConnectorWithDirectSSH(ctx, t)
	defer connector.Close()

	pos, err := connector.GetMasterPos(ctx)
	require.NoError(t, err)
	syncer, _, _, _, err := connector.startCdcStreamingFilePos(ctx, pos) //nolint:dogsled
	require.NoError(t, err)

	// Let CDC streaming establish before closing the syncer.
	time.Sleep(2 * time.Second)

	done := make(chan struct{})
	go func() {
		syncer.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("syncer.Close did not return")
	}
}
