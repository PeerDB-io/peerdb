// Needs the openssh and toxiproxy ancillary services, which run alongside the
// Postgres-source jobs.
//go:build postgres

package utils

import (
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const toxiproxySSHLocalClosePort = 10001

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
