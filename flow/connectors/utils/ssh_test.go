package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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
