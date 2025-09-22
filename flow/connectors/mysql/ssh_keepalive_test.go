package connmysql

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	toxiproxyAPIPort = "18474"
	sshServerPort    = "2222"
	toxiproxyHost    = "localhost"
	sshServerHost    = "openssh"
)

// setupMySQLConnectorWithSSH creates a MySQL connector with SSH tunnel through the given proxy
func setupMySQLConnectorWithSSH(ctx context.Context, t *testing.T, sshProxy *toxiproxy.Proxy) *MySqlConnector {
	t.Helper()

	// Parse port from the Listen address (format: "host:port" or "[::]:port")
	_, portStr, err := net.SplitHostPort(sshProxy.Listen)
	require.NoError(t, err, "Failed to parse proxy listen address: %s", sshProxy.Listen)

	sshPort, err := strconv.Atoi(portStr)
	require.NoError(t, err, "Failed to convert port to integer: %s", portStr)

	connector, err := NewMySqlConnector(ctx, &protos.MySqlConfig{
		Host:     "mysql",
		Port:     3306,
		User:     "root",
		Password: "cipass",
		Database: "mysql",
		SshConfig: &protos.SSHConfig{
			Host:     toxiproxyHost,
			Port:     uint32(sshPort),
			User:     "testuser",
			Password: "testpass",
		},
	})
	require.NoError(t, err)

	// Test initial connection works
	err = connector.ConnectionActive(ctx)
	require.NoError(t, err, "Initial connection should work")

	return connector
}

func TestMySQLSSHKeepaliveWithToxiproxy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := t.Context()

	// Connect to Toxiproxy - NewClient automatically adds http:// if not present
	toxiproxyClient := toxiproxy.NewClient(toxiproxyHost + ":" + toxiproxyAPIPort)

	// Test if toxiproxy is available
	_, err := toxiproxyClient.Version()
	if err != nil {
		t.Skipf("Toxiproxy not available: %v", err)
	}

	// Create proxy from Toxiproxy to the OpenSSH server
	sshProxy, err := toxiproxyClient.CreateProxy("ssh-keepalive-test", "", sshServerHost+":"+sshServerPort)
	require.NoError(t, err)
	defer func() {
		if err := sshProxy.Delete(); err != nil {
			t.Logf("Failed to delete toxiproxy proxy: %v", err)
		}
	}()

	// Create MySQL connector with SSH tunnel through Toxiproxy
	connector := setupMySQLConnectorWithSSH(ctx, t, sshProxy)
	defer connector.Close()

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
	t.Log("Adding 'down' toxic to simulate network failure during long-running query")
	_, err = sshProxy.AddToxic("ssh-down", "down", "", 1.0, toxiproxy.Attributes{})
	require.NoError(t, err)

	// Wait for keepalive failure detection (should happen within ~15-20 seconds)
	t.Log("Waiting for SSH keepalive failure detection...")
	select {
	case <-keepaliveChan:
		t.Log("SSH keepalive failure detected successfully")
		// Channel closing indicates tunnel failure was detected
	case <-time.After(30 * time.Second):
		t.Fatal("SSH keepalive failure not detected within 30 seconds")
	}

	// The long-running query should fail due to broken connection
	select {
	case queryErr := <-queryDone:
		require.Error(t, queryErr, "Long-running query should fail after SSH tunnel failure")
		t.Logf("Long-running query failed as expected: %v", queryErr)
	case <-time.After(10 * time.Second):
		t.Fatal("Long-running query should have failed after SSH tunnel broke")
	}

	// New MySQL operations should succeed because connector will create new SSH tunnel
	err = connector.ConnectionActive(ctx)
	require.NoError(t, err, "New connection should succeed after SSH tunnel recovers")
	t.Log("New connection succeeded - connector created new SSH tunnel as expected")
}

func TestMySQLSSHKeepaliveLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := t.Context()

	toxiproxyClient := toxiproxy.NewClient(toxiproxyHost + ":" + toxiproxyAPIPort)

	// Test if toxiproxy is available
	_, err := toxiproxyClient.Version()
	if err != nil {
		t.Skipf("Toxiproxy not available: %v", err)
	}

	sshProxy, err := toxiproxyClient.CreateProxy("ssh-latency-test", "", sshServerHost+":"+sshServerPort)
	require.NoError(t, err)
	defer func() {
		if err := sshProxy.Delete(); err != nil {
			t.Logf("Failed to delete toxiproxy proxy: %v", err)
		}
	}()

	// Create MySQL connector with SSH tunnel through Toxiproxy
	connector := setupMySQLConnectorWithSSH(ctx, t, sshProxy)
	defer connector.Close()

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
	case <-time.After(45 * time.Second):
		t.Fatal("SSH keepalive timeout not detected within 45 seconds")
	}
}

func TestMySQLSSHResetPeer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := t.Context()

	toxiproxyClient := toxiproxy.NewClient(toxiproxyHost + ":" + toxiproxyAPIPort)

	// Test if toxiproxy is available
	_, err := toxiproxyClient.Version()
	if err != nil {
		t.Skipf("Toxiproxy not available: %v", err)
	}

	sshProxy, err := toxiproxyClient.CreateProxy("ssh-reset-test", "", sshServerHost+":"+sshServerPort)
	require.NoError(t, err)
	defer func() {
		if err := sshProxy.Delete(); err != nil {
			t.Logf("Failed to delete toxiproxy proxy: %v", err)
		}
	}()

	// Create MySQL connector with SSH tunnel through Toxiproxy
	connector := setupMySQLConnectorWithSSH(ctx, t, sshProxy)
	defer connector.Close()

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
			if assert.Contains(t, errorMsg, expected) {
				hasExpectedError = true
				break
			}
		}
		assert.True(t, hasExpectedError, "Error should indicate immediate connection failure, got: %s", errorMsg)

	case <-time.After(15 * time.Second):
		t.Fatal("Query should have failed quickly due to connection reset, not timeout")
	}

	// The keepalive channel may or may not close since the connection failed so quickly
	// that keepalives might not have had a chance to detect it
	select {
	case <-keepaliveChan:
		t.Log("Keepalive channel closed (connection failure detected)")
	case <-time.After(2 * time.Second):
		t.Log("Keepalive channel didn't close - connection failed too quickly for keepalive detection")
	}
}
