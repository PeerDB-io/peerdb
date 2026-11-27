package connclickhouse

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// pingConn simulates a ClickHouse connection whose DELETE takes deleteDuration
// (or blocks until cancelled when deleteDuration is 0) and whose Ping returns pingErr.
type pingConn struct {
	driver.Conn
	pingErr        error
	deleteDuration time.Duration
	pings          atomic.Int32
}

func (c *pingConn) Exec(ctx context.Context, _ string, _ ...any) error {
	if c.deleteDuration == 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.deleteDuration):
		return nil
	}
}

func (c *pingConn) Ping(context.Context) error {
	c.pings.Add(1)
	return c.pingErr
}

func newPingTestConnector(conn *pingConn) *ClickHouseConnector {
	return &ClickHouseConnector{
		database: conn,
		logger:   log.NewStructuredLogger(slog.New(slog.DiscardHandler)),
		Config:   &protos.ClickhouseConfig{},
	}
}

func removeTablesInput() *protos.RemoveTablesFromRawTableInput {
	return &protos.RemoveTablesFromRawTableInput{
		FlowJobName:           "test_flow",
		DestinationTableNames: []string{"t1"},
		NormalizeBatchId:      1,
		SyncBatchId:           2,
	}
}

// synctest.Test fails if any goroutine started inside the bubble is still running when
// the test function returns, so each test also checks that the pinger stops.

func TestRemoveTableEntriesFromRawTablePingFailureCancelsDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pingErr := errors.New("connection reset")
		conn := &pingConn{pingErr: pingErr}
		c := newPingTestConnector(conn)

		start := time.Now()
		err := c.RemoveTableEntriesFromRawTable(t.Context(), removeTablesInput())

		require.ErrorIs(t, err, pingErr)
		require.Equal(t, clickHouseLightweightPingPeriod, time.Since(start), "DELETE should be cancelled by the first failed ping")
		require.Equal(t, int32(1), conn.pings.Load())
	})
}

func TestRemoveTableEntriesFromRawTablePingSuccessKeepsDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &pingConn{deleteDuration: 3*clickHouseLightweightPingPeriod + clickHouseLightweightPingPeriod/2}
		c := newPingTestConnector(conn)

		require.NoError(t, c.RemoveTableEntriesFromRawTable(t.Context(), removeTablesInput()))
		require.Equal(t, int32(3), conn.pings.Load())

		// The pinger must have stopped: no more pings after the function returned.
		time.Sleep(5 * clickHouseLightweightPingPeriod)
		synctest.Wait()
		require.Equal(t, int32(3), conn.pings.Load())
	})
}

func TestRemoveTableEntriesFromRawTableParentCancelKeepsOriginalError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &pingConn{pingErr: errors.New("should not be reached")}
		c := newPingTestConnector(conn)

		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(clickHouseLightweightPingPeriod/2, cancel)

		err := c.RemoveTableEntriesFromRawTable(ctx, removeTablesInput())

		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, conn.pings.Load())
	})
}
