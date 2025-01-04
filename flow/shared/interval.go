package shared

import (
	"context"
	"time"

	"go.temporal.io/sdk/log"
)

func Interval(
	ctx context.Context,
	freq time.Duration,
	fn func(),
) func() {
	shutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(freq)
		defer ticker.Stop()

		for {
			fn()
			select {
			case <-shutdown:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() { close(shutdown) }
}

func IntervalWithLogger(
	ctx context.Context,
	freq time.Duration,
	fn func(),
	logger log.Logger,
) func() {
	shutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(freq)
		defer ticker.Stop()

		for {
			fn()
			select {
			case <-shutdown:
				logger.Info("[heartbeat] interval shutdown requested")
				return
			case <-ctx.Done():
				logger.Info("[heartbeat] context cancelled")
				return
			case <-ticker.C:
			}
		}
	}()
	return func() { close(shutdown) }
}
