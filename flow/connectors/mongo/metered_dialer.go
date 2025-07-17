package connmongo

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/client"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
)

type meteredConn struct {
	utils.NoDeadlineConn
	bytesRead *atomic.Int64
}

func (mc *meteredConn) Read(b []byte) (int, error) {
	read, err := mc.Conn.Read(b)
	mc.bytesRead.Add(int64(read))
	return read, err
}

type meteredDialer struct {
	bytesRead   *atomic.Int64
	innerDialer client.Dialer
}

func (md *meteredDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := md.innerDialer(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &meteredConn{
		NoDeadlineConn: utils.NoDeadlineConn{Conn: conn},
		bytesRead:      md.bytesRead,
	}, nil
}

func newMeteredDialer(bytesRead *atomic.Int64, innerDialer client.Dialer) meteredDialer {
	return meteredDialer{
		bytesRead:   bytesRead,
		innerDialer: innerDialer,
	}
}
