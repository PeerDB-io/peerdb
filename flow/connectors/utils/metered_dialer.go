package utils

import (
	"context"
	"net"
	"sync/atomic"
)

type MeteredConn struct {
	net.Conn
	bytesRead *atomic.Int64
}

func (mc *MeteredConn) Read(b []byte) (int, error) {
	read, err := mc.Conn.Read(b)
	mc.bytesRead.Add(int64(read))
	return read, err
}

type innerDialer func(ctx context.Context, network, address string) (net.Conn, error)

type MeteredDialer struct {
	bytesRead          *atomic.Int64
	innerDialer        innerDialer
	noDeadlineRequired bool
}

func NewMeteredDialer(bytesRead *atomic.Int64,
	innerDialer innerDialer, noDeadlineRequired bool,
) MeteredDialer {
	return MeteredDialer{
		bytesRead:          bytesRead,
		innerDialer:        innerDialer,
		noDeadlineRequired: noDeadlineRequired,
	}
}

func (md *MeteredDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := md.innerDialer(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if md.noDeadlineRequired {
		return &MeteredConn{
			Conn:      &NoDeadlineConn{Conn: conn},
			bytesRead: md.bytesRead,
		}, nil
	} else {
		return &MeteredConn{
			Conn:      conn,
			bytesRead: md.bytesRead,
		}, nil
	}
}
