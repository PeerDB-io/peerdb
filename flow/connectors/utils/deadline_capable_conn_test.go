package utils

import (
	"errors"
	"io"
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

func TestNewDeadlineCapableConn_LocalCloseClosesSSHAndRemoteSide(t *testing.T) {
	sshConn, serverConn := net.Pipe()
	conn := NewDeadlineCapableConn(sshConn)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = serverConn.Close()
	})

	require.NoError(t, conn.Close())

	sshDone := make(chan error, 1)
	go func() {
		_, err := sshConn.Read(make([]byte, 1))
		sshDone <- err
	}()

	serverDone := make(chan error, 1)
	go func() {
		_, err := serverConn.Read(make([]byte, 1))
		serverDone <- err
	}()

	select {
	case err := <-sshDone:
		// since pipe is closed by localConn, we don't actually care about the
		// error type from the sshConn just need to know that it's closed.
		// Showing error type here just to be informative.
		require.ErrorIs(t, err, io.ErrClosedPipe)
	case <-time.After(time.Second):
		t.Fatal("local close did not close SSH side")
	}

	select {
	case err := <-serverDone:
		// since pipe is closed by localConn, we don't actually care about the
		// error type from the sshConn just need to know that it's closed.
		// Showing error type here just to be informative.
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("local close did not close server side")
	}
}

func TestNewDeadlineCapableConn_SSHCloseClosesLocalAndRemote(t *testing.T) {
	sshConn, serverConn := net.Pipe()
	conn := NewDeadlineCapableConn(sshConn)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = serverConn.Close()
	})

	require.NoError(t, sshConn.Close())

	localDone := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		localDone <- err
	}()

	serverDone := make(chan error, 1)
	go func() {
		_, err := serverConn.Read(make([]byte, 1))
		serverDone <- err
	}()

	select {
	case err := <-localDone:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("SSH close did not close local side")
	}

	select {
	case err := <-serverDone:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("SSH close did not close server side")
	}
}

func TestNewDeadlineCapableConn_RemoteCloseClosesSSHAndLocalSide(t *testing.T) {
	sshConn, serverConn := net.Pipe()
	conn := NewDeadlineCapableConn(sshConn)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = serverConn.Close()
	})

	require.NoError(t, serverConn.Close())

	sshDone := make(chan error, 1)
	go func() {
		_, err := sshConn.Read(make([]byte, 1))
		sshDone <- err
	}()

	localDonn := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		localDonn <- err
	}()

	select {
	case err := <-sshDone:
		// ServerConn.Close yields io.EOF on sshConn. The wrapper's io.Copy(proxyConn, sshConn)
		// goroutine observes EOF, and calls sshConn.Close(). sshConn.Read here races with
		// the teardown, so either io.EOF or io.ErrClosedPipe is valid. we don't actually care
		// about the error type from the sshConn just need to know that it's closed. Showing
		// error type here just to be informative.
		require.True(t, errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe),
			"expected io.EOF or io.ErrClosedPipe, got %v", err)
	case <-time.After(time.Second):
		t.Fatal("remote close did not close local side")
	}

	select {
	case err := <-localDonn:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("remote close did not close server side")
	}
}
