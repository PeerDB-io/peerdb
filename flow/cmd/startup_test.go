package cmd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectWithRetrySucceedsImmediately(t *testing.T) {
	t.Parallel()

	attempts := 0
	got, err := connectWithRetry(t.Context(), "test", func(context.Context) (int, error) {
		attempts++
		return 42, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 42, got)
	assert.Equal(t, 1, attempts)
}

func TestConnectWithRetrySucceedsAfterFailure(t *testing.T) {
	t.Parallel()

	attempts := 0
	got, err := connectWithRetry(t.Context(), "test", func(context.Context) (int, error) {
		attempts++
		if attempts < 2 {
			return 0, errors.New("not ready")
		}
		return 42, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 42, got)
	assert.Equal(t, 2, attempts)
}

func TestConnectWithRetryStopsOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	connectErr := errors.New("not ready")
	attempts := 0
	_, err := connectWithRetry(ctx, "test", func(context.Context) (int, error) {
		attempts++
		cancel()
		return 0, connectErr
	})
	require.ErrorIs(t, err, connectErr)
	assert.Equal(t, 1, attempts)
}

func TestConnectWithRetryBoundsEachAttempt(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var attemptDeadline time.Time
	_, err := connectWithRetry(ctx, "test", func(attemptCtx context.Context) (int, error) {
		attemptDeadline, _ = attemptCtx.Deadline()
		cancel()
		return 0, errors.New("not ready")
	})
	require.Error(t, err)
	assert.WithinDuration(t, time.Now().Add(startupRetryAttemptTimeout), attemptDeadline, time.Second)
}
