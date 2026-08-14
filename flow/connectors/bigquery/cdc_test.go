package connbigquery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPollWindow(t *testing.T) {
	checkpoint := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	t.Run("caps at queryWindow past checkpoint when now is far ahead", func(t *testing.T) {
		now := checkpoint.Add(queryWindow * 10)
		upper, ok := pollWindow(checkpoint, now)
		require.True(t, ok)
		assert.True(t, upper.Equal(checkpoint.Add(queryWindow)))
	})

	t.Run("caps at safetyLag behind now when now is close", func(t *testing.T) {
		now := checkpoint.Add(time.Hour)
		upper, ok := pollWindow(checkpoint, now)
		require.True(t, ok)
		assert.True(t, upper.Equal(now.Add(-safetyLag)))
	})

	t.Run("nothing new to scan when safety lag hasn't cleared", func(t *testing.T) {
		now := checkpoint.Add(safetyLag / 2)
		upper, ok := pollWindow(checkpoint, now)
		assert.False(t, ok)
		// upper is still reported (as now-safetyLag), just not usable, since it
		// doesn't move past checkpoint.
		assert.True(t, upper.Equal(now.Add(-safetyLag)))
		assert.False(t, upper.After(checkpoint))
	})

	t.Run("exactly at the boundary is not ok (upper must strictly move past checkpoint)", func(t *testing.T) {
		now := checkpoint.Add(safetyLag)
		upper, ok := pollWindow(checkpoint, now)
		assert.False(t, ok)
		assert.True(t, upper.Equal(checkpoint))
	})
}

func TestWaitForNextPoll(t *testing.T) {
	t.Run("first-ever call (zero lastPollAt) returns immediately", func(t *testing.T) {
		c := &BigQueryConnector{}
		start := time.Now()
		err := c.waitForNextPoll(context.Background(), time.Hour)
		require.NoError(t, err)
		assert.Less(t, time.Since(start), 100*time.Millisecond)
	})

	t.Run("waits out the remainder of idleTimeout since lastPollAt", func(t *testing.T) {
		c := &BigQueryConnector{lastPollAt: time.Now()}
		idleTimeout := 50 * time.Millisecond
		start := time.Now()
		err := c.waitForNextPoll(context.Background(), idleTimeout)
		require.NoError(t, err)
		elapsed := time.Since(start)
		assert.GreaterOrEqual(t, elapsed, idleTimeout-5*time.Millisecond)
	})

	t.Run("no wait once idleTimeout has already elapsed", func(t *testing.T) {
		c := &BigQueryConnector{lastPollAt: time.Now().Add(-time.Hour)}
		start := time.Now()
		err := c.waitForNextPoll(context.Background(), time.Second)
		require.NoError(t, err)
		assert.Less(t, time.Since(start), 100*time.Millisecond)
	})

	t.Run("context cancellation interrupts the wait", func(t *testing.T) {
		c := &BigQueryConnector{lastPollAt: time.Now()}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		err := c.waitForNextPoll(ctx, time.Hour)
		require.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(start), time.Second)
	})
}
