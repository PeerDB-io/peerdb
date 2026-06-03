package connpostgres

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func setupPostgresConnectorWithDirectSSH(ctx context.Context, t *testing.T) *PostgresConnector {
	t.Helper()

	sshPort, err := strconv.ParseUint(utils.SSHServerPort, 10, 32)
	require.NoError(t, err)

	pgConfig := internal.GetAncillaryPostgresConfigFromEnv()
	// The database connection is opened from inside the SSH server container; In CI,
	// PG_HOST=localhost only works for runner-to-PG connections; but from inside the
	// SSH server container, host must be resolvable by the SSH container.
	pgConfig.Host = internal.PostgresToxiproxyUpstreamHostWithFallback(pgConfig.Host)
	pgConfig.SshConfig = &protos.SSHConfig{
		Host:     "localhost",
		Port:     uint32(sshPort),
		User:     "testuser",
		Password: "testpass",
	}

	connector, err := NewPostgresConnector(ctx, nil, pgConfig)
	require.NoError(t, err)

	require.NoError(t, connector.ConnectionActive(ctx), "Initial connection should work")

	return connector
}

func TestPostgresSSHReceiveMessageDeadlineKeepsConnectionUsable(t *testing.T) {
	connector := setupPostgresConnectorWithDirectSSH(t.Context(), t)
	defer connector.Close()

	receiveCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := connector.conn.PgConn().ReceiveMessage(receiveCtx)
		errCh <- err
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(time.Second):
		_ = connector.Close()
		t.Fatal("ReceiveMessage did not respect context deadline over SSH")
	}
	require.Error(t, err)
	require.True(t, pgconn.Timeout(err))
	require.ErrorIs(t, err, context.DeadlineExceeded)

	keepaliveChan := connector.ssh.GetKeepaliveChan(t.Context())
	select {
	case <-keepaliveChan:
		t.Fatal("read deadline timeout should not mark the SSH tunnel unhealthy")
	default:
	}

	require.NoError(t, connector.ConnectionActive(t.Context()), "connection should remain usable after read timeout")
}
