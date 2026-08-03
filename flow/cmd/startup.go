package cmd

import (
	"context"
	"log/slog"
	"time"
)

const (
	startupRetryBudget         = 3 * time.Minute
	startupRetryAttemptTimeout = 30 * time.Second
	startupRetryInitialWait    = 1 * time.Second
	startupRetryMaxWait        = 10 * time.Second
)

func connectWithRetry[T any](ctx context.Context, dependency string, connect func(context.Context) (T, error)) (T, error) {
	deadline := time.Now().Add(startupRetryBudget)
	wait := startupRetryInitialWait
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, startupRetryAttemptTimeout)
		result, err := connect(attemptCtx)
		cancel()
		if err == nil {
			if attempt > 1 {
				slog.InfoContext(ctx, "Startup dependency reachable",
					slog.String("dependency", dependency), slog.Int("attempt", attempt))
			}
			return result, nil
		}
		if ctx.Err() != nil || !time.Now().Add(wait).Before(deadline) {
			var zero T
			return zero, err
		}

		slog.WarnContext(ctx, "Startup dependency unreachable, retrying",
			slog.String("dependency", dependency), slog.Int("attempt", attempt),
			slog.Duration("retryIn", wait), slog.Any("error", err))
		select {
		case <-ctx.Done():
			var zero T
			return zero, err
		case <-time.After(wait):
		}
		wait = min(wait*2, startupRetryMaxWait)
	}
}
