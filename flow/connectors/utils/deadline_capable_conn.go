package utils

import (
	"io"
	"net"
	"sync"
)

type deadlineCapableConn struct {
	net.Conn

	proxyConn net.Conn
	sshConn   net.Conn
	closeOnce sync.Once
}

// NewDeadlineCapableConn creates connection that always respect context deadline.
// Used for ssh connection where deadline is not supported and an error is returned
// when attempting to set deadline (https://github.com/golang/go/issues/65930)
//
// Previously, we implemented this workaround:
// https://github.com/jackc/pgx/issues/382#issuecomment-1496586216
// which silently ignores deadline instead of erroring. However, for connectors
// that depend on context deadline (e.g. Postgres), this means reading from
// ssh channel blocks indefinitely when no message arrives.
//
// The fix here is introducing a deadline-capable in-memory transport between
// the connector and SSH using net.Pipe(). When deadline fires, any chunk the
// io.Copy goroutine already read from SSH is served on next read, avoiding
// data loss.
func NewDeadlineCapableConn(sshConn net.Conn) net.Conn {
	localConn, proxyConn := net.Pipe()

	dcConn := &deadlineCapableConn{
		Conn:      localConn,
		proxyConn: proxyConn,
		sshConn:   sshConn,
	}

	go func() {
		_, _ = io.Copy(dcConn.proxyConn, dcConn.sshConn)
		dcConn.closeAll()
	}()
	go func() {
		_, _ = io.Copy(dcConn.sshConn, dcConn.proxyConn)
		dcConn.closeAll()
	}()

	return dcConn
}

func (c *deadlineCapableConn) Close() error {
	err := c.Conn.Close()
	c.closeAll()
	return err
}

func (c *deadlineCapableConn) closeAll() {
	c.closeOnce.Do(func() {
		_ = c.proxyConn.Close()
		_ = c.sshConn.Close()
	})
}
