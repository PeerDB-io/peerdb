package utils

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewDeadlineCapableConn_ReadDeadlineCanBeCleared(t *testing.T) {
	sshConn, serverConn := net.Pipe()
	conn := NewDeadlineCapableConn(sshConn)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = serverConn.Close()
	})

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Millisecond)))

	buf := make([]byte, 5)
	n, err := conn.Read(buf)
	require.Zero(t, n)
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)

	require.NoError(t, conn.SetReadDeadline(time.Time{}))

	writeDone := make(chan error, 1)
	go func() {
		_, err := serverConn.Write([]byte("hello"))
		writeDone <- err
	}()

	n, err = conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "hello", string(buf[:n]))
	require.NoError(t, <-writeDone)
}

func TestNewDeadlineCapableConn_PreservesSSHBytesAcrossReadTimeout(t *testing.T) {
	sshConn, serverConn := net.Pipe()
	conn := NewDeadlineCapableConn(sshConn)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = serverConn.Close()
	})

	// Wait until the server-side write completes. At that point the proxy has
	// already read the bytes from sshConn and is blocked writing them into the
	// driver-facing pipe because conn has not read yet.
	writeDone := make(chan error, 1)
	go func() {
		_, err := serverConn.Write([]byte("hello"))
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for proxy to read bytes from SSH side")
	}

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(-time.Second)))

	buf := make([]byte, 5)
	n, err := conn.Read(buf)
	require.Zero(t, n)
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)

	require.NoError(t, conn.SetReadDeadline(time.Time{}))

	n, err = conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "hello", string(buf[:n]))
}
