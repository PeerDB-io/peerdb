package connpostgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	toxiproxyDownProxyPort    = 49001
	toxiproxyLatencyProxyPort = 49002
	toxiproxyResetProxyPort   = 49003
)

func setupPostgresConnectorWithSSH(ctx context.Context, t *testing.T, proxyName string, proxyPort int,
) (*PostgresConnector, utils.SSHKeepaliveTestConfig) {
	t.Helper()

	toxiproxyClient := utils.NewToxiproxyClient(t)
	sshProxy := utils.CreateSSHProxy(t, toxiproxyClient, proxyName, proxyPort)

	connector, err := NewPostgresConnector(ctx, nil, &protos.PostgresConfig{
		Host:     "catalog",
		Port:     5432,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
		SshConfig: &protos.SSHConfig{
			Host:     "localhost",
			Port:     uint32(proxyPort),
			User:     "testuser",
			Password: "testpass",
		},
	})
	require.NoError(t, err)

	// Test initial connection works
	err = connector.ConnectionActive(ctx)
	require.NoError(t, err, "Initial connection should work")

	keepaliveChan := connector.ssh.GetKeepaliveChan(ctx)

	return connector, utils.SSHKeepaliveTestConfig{
		SSHProxy:      sshProxy,
		KeepaliveChan: keepaliveChan,
		RunLongQuery: func(ctx context.Context) error {
			_, err := connector.conn.Exec(ctx, "SELECT pg_sleep(60)")
			return err
		},
	}
}

func TestPostgresSSHKeepaliveWithToxiproxy(t *testing.T) {
	connector, cfg := setupPostgresConnectorWithSSH(t.Context(), t, "pg-ssh-keepalive-test", toxiproxyDownProxyPort)
	defer connector.Close()
	utils.RunSSHKeepaliveDownTest(t, cfg)
}

func TestPostgresSSHKeepaliveLatency(t *testing.T) {
	connector, cfg := setupPostgresConnectorWithSSH(t.Context(), t, "pg-ssh-latency-test", toxiproxyLatencyProxyPort)
	defer connector.Close()
	utils.RunSSHKeepaliveLatencyTest(t, cfg)
}

func TestPostgresSSHResetPeer(t *testing.T) {
	connector, cfg := setupPostgresConnectorWithSSH(t.Context(), t, "pg-ssh-reset-peer-test", toxiproxyResetProxyPort)
	defer connector.Close()
	utils.RunSSHResetPeerTest(t, cfg)
}
