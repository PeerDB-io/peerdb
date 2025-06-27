package connmysql

/* go-mysql does not expose raw bytes for streaming selects,
 * thus this allows accurately measuring fetched bytes */

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/client"
)

type MeteredConn struct {
	net.Conn
	BytesRead atomic.Int64
}

func (mc *MeteredConn) Read(b []byte) (int, error) {
	read, err := mc.Conn.Read(b)
	mc.BytesRead.Add(int64(read))
	return read, err
}

func NewMeteredDialer(innerDialer client.Dialer) client.Dialer {
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		conn, err := innerDialer(ctx, network, address)
		if err != nil {
			return conn, err
		}
		return &MeteredConn{Conn: conn}, nil
	}
}

func BytesReadFromMySqlConn(conn *client.Conn) *atomic.Int64 {
	// during tls auth go-mysql wraps our MeteredConn in a tls.Conn
	if mc, ok := conn.Conn.Conn.(*MeteredConn); ok {
		return &mc.BytesRead
	} else if tlsConn, ok := conn.Conn.Conn.(*tls.Conn); ok {
		if mc, ok := tlsConn.NetConn().(*MeteredConn); ok {
			return &mc.BytesRead
		}
	}
	return nil
}
