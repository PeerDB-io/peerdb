package utils

import (
	"context"
	"net"
	"sync/atomic"
)

type MeteredConn struct {
	net.Conn
	totalBytesRead *atomic.Int64
	deltaBytesRead *atomic.Int64
}

func (mc *MeteredConn) Read(b []byte) (int, error) {
	read, err := mc.Conn.Read(b)
	mc.totalBytesRead.Add(int64(read))
	mc.deltaBytesRead.Add(int64(read))
	return read, err
}

type innerDialer func(ctx context.Context, network, address string) (net.Conn, error)

type MeteredDialer struct {
	totalBytesRead     *atomic.Int64
	deltaBytesRead     *atomic.Int64
	innerDialer        innerDialer
	noDeadlineRequired bool
}

func NewMeteredDialer(totalBytesRead *atomic.Int64, deltaBytesRead *atomic.Int64,
	innerDialer innerDialer, noDeadlineRequired bool,
) MeteredDialer {
	return MeteredDialer{
		totalBytesRead:     totalBytesRead,
		deltaBytesRead:     deltaBytesRead,
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
			Conn:           &NoDeadlineConn{Conn: conn},
			totalBytesRead: md.totalBytesRead,
			deltaBytesRead: md.deltaBytesRead,
		}, nil
	} else {
		return &MeteredConn{
			Conn:           conn,
			totalBytesRead: md.totalBytesRead,
			deltaBytesRead: md.deltaBytesRead,
		}, nil
	}
}
