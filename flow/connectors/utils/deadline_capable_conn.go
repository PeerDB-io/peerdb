package utils

import (
	"io"
	"net"
	"sync"
)

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

	var closeOnce sync.Once
	closeAll := func() {
		closeOnce.Do(func() {
			// we do not explicitly close localConn here
			// as that is the responsibility of the caller
			_ = proxyConn.Close()
			_ = sshConn.Close()
		})
	}

	go func() {
		_, _ = io.Copy(proxyConn, sshConn)
		closeAll()
	}()
	go func() {
		_, _ = io.Copy(sshConn, proxyConn)
		closeAll()
	}()

	return localConn
}
