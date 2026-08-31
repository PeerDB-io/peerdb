package clickhouse

import (
	"context"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

// retryableErrConn always fails Exec/Query with a retryable ClickHouse exception, triggering retry loop.
type retryableErrConn struct {
	driver.Conn
}

func (retryableErrConn) Exec(context.Context, string, ...any) error {
	return &clickhouse.Exception{Code: int32(chproto.ErrTooManyParts)}
}

func (retryableErrConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, &clickhouse.Exception{Code: int32(chproto.ErrTooManyParts)}
}

func TestExecRetryBackoffPreemptedByContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := Exec(ctx, nopLogger{}, retryableErrConn{}, "INSERT INTO t VALUES (1)")
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, time.Second, "cancellation should preempt the retry backoff")
}

func TestQueryRetryBackoffPreemptedByContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	rows, err := Query(ctx, nopLogger{}, retryableErrConn{}, "SELECT 1")
	elapsed := time.Since(start)

	require.Nil(t, rows)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, time.Second, "cancellation should preempt the retry backoff")
}
