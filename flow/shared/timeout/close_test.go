package timeout

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCloseWithTimeout_FastClose(t *testing.T) {
	var onTimeoutCalls atomic.Int32
	CloseWithTimeout(
		func() {}, // returns immediately
		time.Second,
		func() { onTimeoutCalls.Add(1) },
	)
	require.Zero(t, onTimeoutCalls.Load())
}

func TestCloseWithTimeout_ClosureHangs(t *testing.T) {
	var onTimeoutCalls atomic.Int32
	release := make(chan struct{})
	defer close(release)

	timeout := 50 * time.Millisecond
	start := time.Now()
	CloseWithTimeout(
		func() { <-release }, // blocks
		timeout,
		func() { onTimeoutCalls.Add(1) },
	)
	elapsed := time.Since(start)

	require.Equal(t, int32(1), onTimeoutCalls.Load())
	require.GreaterOrEqual(t, elapsed, timeout)
	require.Less(t, elapsed, timeout+time.Second)
}
