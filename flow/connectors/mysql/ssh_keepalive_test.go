package connmysql

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	toxiproxyDownProxyPort    = 42001
	toxiproxyLatencyProxyPort = 42002
	toxiproxyResetProxyPort   = 42003
)

func setupMySQLConnectorWithSSH(ctx context.Context, t *testing.T, proxyName string, proxyPort int,
) (*MySqlConnector, utils.SSHKeepaliveTestConfig) {
	t.Helper()

	toxiproxyClient := utils.NewToxiproxyClient(t)
	sshProxy := utils.CreateSSHProxy(t, toxiproxyClient, proxyName, proxyPort)

	mysqlHost := "mysql"
	if envHost := os.Getenv("CI_MYSQL_HOST"); envHost != "" {
		mysqlHost = envHost
	}

	var mysqlPort uint32 = 3306
	if envPortStr := os.Getenv("CI_MYSQL_PORT"); envPortStr != "" {
		envPort, err := strconv.ParseUint(strings.TrimSpace(strings.Split(envPortStr, "#")[0]), 10, 32)
		require.NoError(t, err, "Failed to parse CI_MYSQL_PORT")
		mysqlPort = uint32(envPort)
	}

	mysqlRootPass := "cipass"
	if envPass := os.Getenv("CI_MYSQL_ROOT_PASSWORD"); envPass != "" {
		mysqlRootPass = envPass
	}

	connector, err := NewMySqlConnector(ctx, &protos.MySqlConfig{
		Host:     mysqlHost,
		Port:     mysqlPort,
		User:     "root",
		Password: mysqlRootPass,
		Database: "mysql",
		SshConfig: &protos.SSHConfig{
			Host:     "localhost",
			Port:     uint32(proxyPort),
			User:     "testuser",
			Password: "testpass",
		},
		DisableTls: true,
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
			_, err := connector.Execute(ctx, "SELECT SLEEP(60)")
			return err
		},
	}
}

func TestMySQLSSHKeepaliveWithToxiproxy(t *testing.T) {
	if os.Getenv("CI_MYSQL_VERSION") == "maria" {
		t.Skip("Skipping SSH keepalive test for MariaDB")
	}
	connector, cfg := setupMySQLConnectorWithSSH(t.Context(), t, "my-ssh-keepalive-test", toxiproxyDownProxyPort)
	defer connector.Close()
	utils.RunSSHKeepaliveDownTest(t, cfg)
}

func TestMySQLSSHKeepaliveLatency(t *testing.T) {
	if os.Getenv("CI_MYSQL_VERSION") == "maria" {
		t.Skip("Skipping SSH keepalive test for MariaDB")
	}
	connector, cfg := setupMySQLConnectorWithSSH(t.Context(), t, "my-ssh-latency-test", toxiproxyLatencyProxyPort)
	defer connector.Close()
	utils.RunSSHKeepaliveLatencyTest(t, cfg)
}

func TestMySQLSSHResetPeer(t *testing.T) {
	if os.Getenv("CI_MYSQL_VERSION") == "maria" {
		t.Skip("Skipping SSH keepalive test for MariaDB")
	}
	connector, cfg := setupMySQLConnectorWithSSH(t.Context(), t, "my-ssh-reset-peer-test", toxiproxyResetProxyPort)
	defer connector.Close()
	utils.RunSSHResetPeerTest(t, cfg)
}
