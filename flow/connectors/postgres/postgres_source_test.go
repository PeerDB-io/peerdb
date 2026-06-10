package connpostgres

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
)

func TestReplConnKeepalive(t *testing.T) {
	t.Parallel()

	c := &PostgresConnector{
		logger: log.NewStructuredLogger(slog.New(slog.DiscardHandler)),
	}
	c.pullActive.Store(true)

	var pings atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		c.runReplConnKeepalive(ctx, 5*time.Millisecond, func(context.Context) error {
			pings.Add(1)
			return nil
		})
		close(done)
	}()

	// Active phase: pings should be suppressed.
	time.Sleep(30 * time.Millisecond)
	require.Zero(t, pings.Load())

	// Idle phase: pings should fire on every tick.
	c.pullActive.Store(false)
	time.Sleep(30 * time.Millisecond)
	afterIdle := pings.Load()
	require.Positive(t, afterIdle)

	// Cancel ctx and confirm the goroutine exits promptly.
	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("keepalive goroutine did not exit after ctx cancel")
	}
}
