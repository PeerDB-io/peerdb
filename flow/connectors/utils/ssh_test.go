package utils

import (
	"context"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const toxiproxySSHLocalClosePort = 10001

func TestSSHTunnel_GetKeepaliveChan_NilCases(t *testing.T) {
	// Nil tunnel
	var tunnel *SSHTunnel = nil
	require.Nil(t, tunnel.GetKeepaliveChan(context.Background()), "Nil tunnel should return nil channel")

	// Nil client
	tunnel = &SSHTunnel{Client: nil}
	require.Nil(t, tunnel.GetKeepaliveChan(context.Background()), "Tunnel with nil client should return nil channel")

	// Bad tunnel
	tunnel = &SSHTunnel{Client: nil, badTunnel: true}
	require.Nil(t, tunnel.GetKeepaliveChan(context.Background()), "Bad tunnel should return nil channel")
}

func TestSSHTunnel_StartKeepalive_NilCases(t *testing.T) {
	called := false
	onFailure := func() { called = true }

	var tunnel *SSHTunnel = nil
	tunnel.StartKeepalive(context.Background(), onFailure)
	require.False(t, called, "Nil tunnel should not call onFailure")

	tunnel = &SSHTunnel{Client: nil}
	tunnel.StartKeepalive(context.Background(), onFailure)
	require.False(t, called, "Tunnel with nil client should not call onFailure")

	tunnel = &SSHTunnel{Client: nil, badTunnel: true}
	tunnel.StartKeepalive(context.Background(), onFailure)
	require.False(t, called, "Bad tunnel should not call onFailure")
}

func TestSSHTunnel_Close_NilCases(t *testing.T) {
	// Nil tunnel
	var tunnel *SSHTunnel = nil
	require.NoError(t, tunnel.Close(), "Closing nil tunnel should not error")

	// Nil client
	tunnel = &SSHTunnel{Client: nil}
	err := tunnel.Close()
	require.NoError(t, err, "Closing tunnel with nil client should not error")

	// Double close
	err2 := tunnel.Close()
	require.NoError(t, err2, "Second close should not error")
}

func TestSSHTunnel_LocalCloseDoesNotBlockDuringSSHHang(t *testing.T) {
	toxiproxyClient := NewToxiproxyClient(t)
	sshProxy := CreateSSHProxy(t, toxiproxyClient, "ssh-hang-local-close-test", toxiproxySSHLocalClosePort)

	tunnel, err := NewSSHTunnel(t.Context(), &protos.SSHConfig{
		Host:     "localhost",
		Port:     toxiproxySSHLocalClosePort,
		User:     "testuser",
		Password: "testpass",
	})
	require.NoError(t, err)
	defer tunnel.Close()

	conn, err := tunnel.DialContext(t.Context(), "tcp", "localhost:"+SSHServerPort)
	require.NoError(t, err)

	_, err = sshProxy.AddToxic("latency", "latency", "", 1.0, toxiproxy.Attributes{
		"latency": 120000,
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- conn.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("local connection close did not return while SSH path was hung")
	}
}
