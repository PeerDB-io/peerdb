package utils

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
)

const (
	ToxiproxyAPIPort = "18474"
	SSHServerPort    = "2222"
	ToxiproxyHost    = "localhost"
	SSHServerHost    = "openssh"
)

// Callbacks avoid adding test-only methods to the connector interfaces in core.go.
type SSHKeepaliveTestConfig struct {
	SSHProxy      *toxiproxy.Proxy
	KeepaliveChan <-chan struct{}
	RunLongQuery  func(ctx context.Context) error
}

func NewToxiproxyClient(t *testing.T) *toxiproxy.Client {
	t.Helper()
	client := toxiproxy.NewClient(ToxiproxyHost + ":" + ToxiproxyAPIPort)
	_, err := client.Version()
	require.NoError(t, err, "Toxiproxy not available")
	return client
}

func CreateSSHProxy(t *testing.T, client *toxiproxy.Client, name string, port int) *toxiproxy.Proxy {
	t.Helper()
	proxy, err := client.CreateProxy(name, "0.0.0.0:"+strconv.Itoa(port), SSHServerHost+":"+SSHServerPort)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := proxy.Delete(); err != nil {
			t.Logf("Failed to delete toxiproxy proxy: %v", err)
		}
	})
	return proxy
}

func RunSSHKeepaliveDownTest(t *testing.T, cfg SSHKeepaliveTestConfig) {
	t.Helper()
	ctx := t.Context()

	require.NotNil(t, cfg.KeepaliveChan, "SSH keepalive channel should exist")

	// Start a long-running query in a goroutine
	queryDone := concurrency.NewLatch[error]()
	go func() {
		queryDone.Set(cfg.RunLongQuery(ctx))
	}()

	// Wait a bit for the query to start
	time.Sleep(2 * time.Second)

	// Simulate network going down - this requires keepalives to detect the failure
	t.Log("Disabling proxy to simulate network failure during long-running query")
	require.NoError(t, cfg.SSHProxy.Disable())

	// Wait for keepalive failure detection (should happen within ~15-20 seconds)
	t.Log("Waiting for SSH keepalive failure detection...")
	select {
	case <-cfg.KeepaliveChan:
		t.Log("SSH keepalive failure detected successfully")
		// Channel closing indicates tunnel failure was detected
	case <-time.After(2 * SSHKeepaliveInterval):
		t.Fatal("SSH keepalive failure not detected within 2 intervals")
	}

	// The long-running query should fail due to broken connection
	select {
	case <-queryDone.Chan():
		queryErr := queryDone.Wait()
		require.Error(t, queryErr, "Long-running query should fail after SSH tunnel failure")
		t.Logf("Long-running query failed as expected: %v", queryErr)
	case <-time.After(10 * time.Second):
		t.Fatal("Long-running query should have failed after SSH tunnel broke")
	}
}

func RunSSHKeepaliveLatencyTest(t *testing.T, cfg SSHKeepaliveTestConfig) {
	t.Helper()

	require.NotNil(t, cfg.KeepaliveChan)

	// Add high latency that should cause keepalive timeouts
	// SSH keepalives happen every 15 seconds, so 25s latency should cause timeout
	t.Log("Adding latency toxic to cause SSH keepalive timeouts")
	_, err := cfg.SSHProxy.AddToxic("latency", "latency", "", 1.0, toxiproxy.Attributes{
		"latency": 25000, // 25 seconds
	})
	require.NoError(t, err, "Failed to add latency toxic")

	// Should detect keepalive failure due to timeout
	t.Log("Waiting for SSH keepalive timeout...")
	select {
	case <-cfg.KeepaliveChan:
		t.Log("SSH keepalive timeout detected successfully")
		// Channel closing indicates tunnel timeout was detected
	case <-time.After(3 * SSHKeepaliveInterval):
		t.Fatal("SSH keepalive timeout not detected within 3 intervals")
	}
}

func RunSSHResetPeerTest(t *testing.T, cfg SSHKeepaliveTestConfig) {
	t.Helper()
	ctx := t.Context()

	require.NotNil(t, cfg.KeepaliveChan)

	// Start a long-running query
	queryDone := concurrency.NewLatch[error]()
	go func() {
		queryDone.Set(cfg.RunLongQuery(ctx))
	}()

	// Wait for query to start
	time.Sleep(2 * time.Second)

	// Use reset_peer toxic - this immediately closes connections, bypassing keepalives
	t.Log("Adding reset_peer toxic - should cause immediate connection failure")
	_, err := cfg.SSHProxy.AddToxic("ssh-reset-peer", "reset_peer", "", 1.0, toxiproxy.Attributes{})
	require.NoError(t, err, "Failed to add reset_peer toxic")

	// The long-running query should fail quickly due to connection reset
	select {
	case <-queryDone.Chan():
		queryErr := queryDone.Wait()
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

	case <-time.After(SSHKeepaliveInterval):
		t.Fatal("Query should have failed quickly due to connection reset, not timeout")
	}
}
