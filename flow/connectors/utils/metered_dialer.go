package utils

import (
	"context"
	"net"
	"sync/atomic"
)

type Meter struct {
	lastValue int64
	bytesRead atomic.Int64
}

func (m *Meter) Reset() {
	m.bytesRead.Store(0)
	m.lastValue = 0
}

func (m *Meter) TotalBytesRead() int64 {
	return m.bytesRead.Load()
}

func (m *Meter) DeltaBytesRead() int64 {
	current := m.bytesRead.Load()
	delta := current - m.lastValue
	m.lastValue = current
	return delta
}

type MeteredConn struct {
	net.Conn
	meter *Meter
}

func (mc *MeteredConn) Read(b []byte) (int, error) {
	read, err := mc.Conn.Read(b)
	mc.meter.bytesRead.Add(int64(read))
	return read, err
}

type innerDialer func(ctx context.Context, network, address string) (net.Conn, error)

type MeteredDialer struct {
	meter              *Meter
	innerDialer        innerDialer
	noDeadlineRequired bool
}

func NewMeteredDialer(meter *Meter,
	innerDialer innerDialer, noDeadlineRequired bool,
) MeteredDialer {
	return MeteredDialer{
		meter:              meter,
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
			Conn:  &NoDeadlineConn{Conn: conn},
			meter: md.meter,
		}, nil
	} else {
		return &MeteredConn{
			Conn:  conn,
			meter: md.meter,
		}, nil
	}
}
