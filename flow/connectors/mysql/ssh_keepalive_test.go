package connmysql

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	toxiproxyAPIPort          = "18474"
	sshServerPort             = "2222"
	toxiproxyHost             = "localhost"
	sshServerHost             = "openssh"
	toxiproxyDownProxyPort    = "42001"
	toxiproxyLatencyProxyPort = "42002"
	toxiproxyResetProxyPort   = "42003"
)

// setupMySQLConnectorWithSSH creates a toxiproxy and MySQL connector with SSH tunnel
func setupMySQLConnectorWithSSH(ctx context.Context, t *testing.T,
	toxiproxyClient *toxiproxy.Client, proxyName string, proxyPort string,
) (*MySqlConnector, *toxiproxy.Proxy) {
	t.Helper()

	// Create proxy from Toxiproxy to the OpenSSH server
	sshProxy, err := toxiproxyClient.CreateProxy(proxyName, "0.0.0.0:"+proxyPort, sshServerHost+":"+sshServerPort)
	require.NoError(t, err)

	sshPort, err := strconv.Atoi(proxyPort)
	require.NoError(t, err, "Failed to convert port to integer: %s", proxyPort)

	connector, err := NewMySqlConnector(ctx, &protos.MySqlConfig{
		Host:     "mysql",
		Port:     3306,
		User:     "root",
		Password: "cipass",
		Database: "mysql",
		SshConfig: &protos.SSHConfig{
			Host:     "localhost",
			Port:     uint32(sshPort),
			User:     "testuser",
			Password: "testpass",
		},
		DisableTls: true,
	})
	require.NoError(t, err)

	// Test initial connection works
	err = connector.ConnectionActive(ctx)
	require.NoError(t, err, "Initial connection should work")

	return connector, sshProxy
}

func TestMySQLSSHKeepaliveWithToxiproxy(t *testing.T) {
	ctx := t.Context()

	toxiproxyClient := toxiproxy.NewClient(toxiproxyHost + ":" + toxiproxyAPIPort)
	_, err := toxiproxyClient.Version()
	require.NoError(t, err, "Toxiproxy not available")

	// Create MySQL connector with SSH tunnel through Toxiproxy
	connector, sshProxy := setupMySQLConnectorWithSSH(ctx, t, toxiproxyClient, "ssh-keepalive-test", toxiproxyDownProxyPort)
	defer connector.Close()
	defer func() {
		if err := sshProxy.Delete(); err != nil {
			t.Logf("Failed to delete toxiproxy proxy: %v", err)
		}
	}()

	// Get the SSH keepalive channel
	keepaliveChan := connector.ssh.GetKeepaliveChan(ctx)
	require.NotNil(t, keepaliveChan, "SSH keepalive channel should exist")

	// Start a long-running query in a goroutine
	queryDone := make(chan error, 1)
	go func() {
		// This query will sleep for 60 seconds, giving us time to break the SSH tunnel
		_, err := connector.Execute(ctx, "SELECT SLEEP(60)")
		queryDone <- err
	}()

	// Wait a bit for the query to start
	time.Sleep(2 * time.Second)

	// Simulate network going down - this requires keepalives to detect the failure
	t.Log("Disabling proxy to simulate network failure during long-running query")
	err = sshProxy.Disable()
	require.NoError(t, err)

	// Wait for keepalive failure detection (should happen within ~15-20 seconds)
	t.Log("Waiting for SSH keepalive failure detection...")
	select {
	case <-keepaliveChan:
		t.Log("SSH keepalive failure detected successfully")
		// Channel closing indicates tunnel failure was detected
	case <-time.After(2 * utils.SSHKeepaliveInterval):
		t.Fatal("SSH keepalive failure not detected within 2 intervals")
	}

	// The long-running query should fail due to broken connection
	select {
	case queryErr := <-queryDone:
		require.Error(t, queryErr, "Long-running query should fail after SSH tunnel failure")
		t.Logf("Long-running query failed as expected: %v", queryErr)
	case <-time.After(10 * time.Second):
		t.Fatal("Long-running query should have failed after SSH tunnel broke")
	}

	// Wait a bit for conn recycle
	time.Sleep(2 * time.Second)

	// New MySQL operations should succeed because connector will create new SSH tunnel
	err = connector.ConnectionActive(ctx)
	require.NoError(t, err, "New connection should succeed after SSH tunnel recovers")
	t.Log("New connection succeeded - connector created new SSH tunnel as expected")
}

func TestMySQLSSHKeepaliveLatency(t *testing.T) {
	ctx := t.Context()

	toxiproxyClient := toxiproxy.NewClient(toxiproxyHost + ":" + toxiproxyAPIPort)
	_, err := toxiproxyClient.Version()
	require.NoError(t, err, "Toxiproxy not available")

	// Create MySQL connector with SSH tunnel through Toxiproxy
	connector, sshProxy := setupMySQLConnectorWithSSH(ctx, t, toxiproxyClient, "ssh-latency-test", toxiproxyLatencyProxyPort)
	defer connector.Close()
	defer func() {
		if err := sshProxy.Delete(); err != nil {
			t.Logf("Failed to delete toxiproxy proxy: %v", err)
		}
	}()

	keepaliveChan := connector.ssh.GetKeepaliveChan(ctx)
	require.NotNil(t, keepaliveChan)

	// Add high latency that should cause keepalive timeouts
	// SSH keepalives happen every 15 seconds, so 25s latency should cause timeout
	t.Log("Adding latency toxic to cause SSH keepalive timeouts")
	_, err = sshProxy.AddToxic("latency", "latency", "", 1.0, toxiproxy.Attributes{
		"latency": 25000, // 25 seconds
	})
	require.NoError(t, err)

	// Should detect keepalive failure due to timeout
	t.Log("Waiting for SSH keepalive timeout...")
	select {
	case <-keepaliveChan:
		t.Log("SSH keepalive timeout detected successfully")
		// Channel closing indicates tunnel timeout was detected
	case <-time.After(2 * utils.SSHKeepaliveInterval):
		t.Fatal("SSH keepalive timeout not detected within 2 intervals")
	}
}

func TestMySQLSSHResetPeer(t *testing.T) {
	ctx := t.Context()

	toxiproxyClient := toxiproxy.NewClient(toxiproxyHost + ":" + toxiproxyAPIPort)
	_, err := toxiproxyClient.Version()
	require.NoError(t, err, "Toxiproxy not available")

	// Create MySQL connector with SSH tunnel through Toxiproxy
	connector, sshProxy := setupMySQLConnectorWithSSH(ctx, t, toxiproxyClient, "ssh-reset-peer-test", toxiproxyResetProxyPort)
	defer connector.Close()
	defer func() {
		if err := sshProxy.Delete(); err != nil {
			t.Logf("Failed to delete toxiproxy proxy: %v", err)
		}
	}()

	keepaliveChan := connector.ssh.GetKeepaliveChan(ctx)
	require.NotNil(t, keepaliveChan)

	// Start a long-running query
	queryDone := make(chan error, 1)
	go func() {
		_, err := connector.Execute(ctx, "SELECT SLEEP(60)")
		queryDone <- err
	}()

	// Wait for query to start
	time.Sleep(2 * time.Second)

	// Use reset_peer toxic - this immediately closes connections, bypassing keepalives
	t.Log("Adding reset_peer toxic - should cause immediate connection failure")
	_, err = sshProxy.AddToxic("ssh-reset-peer", "reset_peer", "", 1.0, toxiproxy.Attributes{})
	require.NoError(t, err)

	// The long-running query should fail quickly due to connection reset
	select {
	case queryErr := <-queryDone:
		require.Error(t, queryErr, "Query should fail due to connection reset")

		// Assert on specific error messages that indicate immediate connection failure
		errorMsg := queryErr.Error()
		t.Logf("Connection reset error: %v", errorMsg)

		// Check for common connection reset error patterns
		hasExpectedError := false
		expectedErrors := []string{"connection reset", "broken pipe", "EOF", "connection closed", "use of closed network connection"}
		for _, expected := range expectedErrors {
			if strings.Contains(errorMsg, expected) {
				hasExpectedError = true
				break
			}
		}
		assert.True(t, hasExpectedError, "Error should indicate immediate connection failure, got: %s", errorMsg)

	case <-time.After(utils.SSHKeepaliveInterval):
		t.Fatal("Query should have failed quickly due to connection reset, not timeout")
	}
}
