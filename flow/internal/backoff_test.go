package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExponentialBackoff(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		ctx := context.Background()
		attempts := 0

		result, err := ExponentialBackoff(ctx, func() (int, error) {
			attempts++
			return 42, nil
		},
			WithBackoffMaxAttempts(3),
			WithBackoffInitialDelay(time.Millisecond),
			WithBackoffRetryable(func(err error) bool { return true }),
		)

		require.NoError(t, err)
		require.Equal(t, 42, result)
		require.Equal(t, 1, attempts)
	})

	t.Run("retries on retryable error", func(t *testing.T) {
		ctx := context.Background()
		attempts := 0
		retryableErr := errors.New("retryable error")

		result, err := ExponentialBackoff(ctx, func() (int, error) {
			attempts++
			if attempts < 3 {
				return 0, retryableErr
			}
			return 42, nil
		},
			WithBackoffMaxAttempts(3),
			WithBackoffInitialDelay(time.Millisecond),
			WithBackoffRetryable(func(err error) bool { return errors.Is(err, retryableErr) }),
		)

		require.NoError(t, err)
		require.Equal(t, 42, result)
		require.Equal(t, 3, attempts)
	})

	t.Run("does not retry on non-retryable error", func(t *testing.T) {
		ctx := context.Background()
		attempts := 0
		nonRetryableErr := errors.New("non-retryable error")

		_, err := ExponentialBackoff(ctx, func() (int, error) {
			attempts++
			return 0, nonRetryableErr
		},
			WithBackoffMaxAttempts(3),
			WithBackoffInitialDelay(time.Millisecond),
			WithBackoffRetryable(func(err error) bool { return false }),
		)

		require.ErrorIs(t, err, nonRetryableErr)
		require.Equal(t, 1, attempts)
	})

	t.Run("returns error after max attempts", func(t *testing.T) {
		ctx := context.Background()
		attempts := 0
		retryableErr := errors.New("always fails")

		_, err := ExponentialBackoff(ctx, func() (int, error) {
			attempts++
			return 0, retryableErr
		},
			WithBackoffMaxAttempts(3),
			WithBackoffInitialDelay(time.Millisecond),
			WithBackoffRetryable(func(err error) bool { return true }),
		)

		require.Error(t, err)
		require.ErrorIs(t, err, retryableErr)
		require.Contains(t, err.Error(), "failed after 3 attempts")
		require.Equal(t, 3, attempts)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		attempts := 0

		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		_, err := ExponentialBackoff(ctx, func() (int, error) {
			attempts++
			return 0, errors.New("error")
		},
			WithBackoffMaxAttempts(3),
			WithBackoffInitialDelay(time.Hour), // Long delay to ensure we hit context cancellation
			WithBackoffRetryable(func(err error) bool { return true }),
		)

		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, attempts)
	})

	t.Run("uses defaults when no options provided", func(t *testing.T) {
		ctx := context.Background()
		attempts := 0

		// With no options, isRetryable defaults to always returning false
		_, err := ExponentialBackoff(ctx, func() (int, error) {
			attempts++
			return 0, errors.New("error")
		})

		require.Error(t, err)
		require.Equal(t, 1, attempts) // No retries because default isRetryable returns false
	})
}
