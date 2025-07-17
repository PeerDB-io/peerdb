package connmysql

/* go-mysql does not expose raw bytes for streaming selects,
 * thus this allows accurately measuring fetched bytes.
 * go-mysql wraps connection with tls.Conn for tls,
 * so one should not try retrieving the original connection.
 * simpler to have the meter write to an atomic int we own.
 */

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/client"
)

type meteredConn struct {
	net.Conn
	bytesRead *atomic.Int64
}

func (mc *meteredConn) Read(b []byte) (int, error) {
	read, err := mc.Conn.Read(b)
	mc.bytesRead.Add(int64(read))
	return read, err
}

func newMeteredDialer(bytesRead *atomic.Int64, innerDialer client.Dialer) client.Dialer {
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		conn, err := innerDialer(ctx, network, address)
		if err != nil {
			return conn, err
		}
		return &meteredConn{Conn: conn, bytesRead: bytesRead}, nil
	}
}
