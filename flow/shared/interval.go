package shared

import (
	"context"
	"time"
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
