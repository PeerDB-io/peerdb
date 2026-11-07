package internal

import (
	"context"
	"fmt"
	"time"
)

// backoffConfig holds configuration for exponential backoff retry logic.
type backoffConfig struct {
	maxAttempts  int
	initialDelay time.Duration
	isRetryable  func(error) bool
}

// BackoffOption configures exponential backoff behavior.
type BackoffOption func(*backoffConfig)

// WithBackoffMaxAttempts sets the maximum number of retry attempts.
// Default is 3 if not specified.
func WithBackoffMaxAttempts(n int) BackoffOption {
	return func(c *backoffConfig) {
		c.maxAttempts = n
	}
}

// WithBackoffInitialDelay sets the initial delay before the first retry.
// The delay doubles with each subsequent attempt (e.g., 2s, 4s, 8s).
// Default is 1 second if not specified.
func WithBackoffInitialDelay(d time.Duration) BackoffOption {
	return func(c *backoffConfig) {
		c.initialDelay = d
	}
}

// WithBackoffRetryable sets the function that determines if an error is retryable.
// If not specified, no errors are retried.
func WithBackoffRetryable(fn func(error) bool) BackoffOption {
	return func(c *backoffConfig) {
		c.isRetryable = fn
	}
}

// ExponentialBackoff executes the given function with exponential backoff retry logic.
// It retries when isRetryable returns true for the error, up to maxAttempts times.
// The delay between retries follows: initialDelay * 2^attempt (e.g., 2s, 4s, 8s)
// Returns the result of fn on success, or the last error after all attempts are exhausted.
func ExponentialBackoff[T any](ctx context.Context, fn func() (T, error), opts ...BackoffOption) (T, error) {
	config := backoffConfig{
		maxAttempts:  3,
		initialDelay: time.Second,
		isRetryable:  func(error) bool { return false },
	}
	for _, opt := range opts {
		opt(&config)
	}

	var zero T
	var lastErr error

	for attempt := range config.maxAttempts {
		result, err := fn()
		if err == nil {
			return result, nil
		}

		if !config.isRetryable(err) {
			return zero, err
		}

		lastErr = err

		// Don't sleep after the last attempt
		if attempt < config.maxAttempts-1 {
			delay := config.initialDelay * time.Duration(1<<attempt)
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return zero, fmt.Errorf("failed after %d attempts: %w", config.maxAttempts, lastErr)
}
