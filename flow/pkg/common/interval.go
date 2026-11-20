package common

import (
	"context"
	"math/rand/v2"
	"time"
)

type intervalOptions struct {
	jitter bool
}

type IntervalOption func(*intervalOptions)

// WithIntervalJitter delays the first call by a random duration in [0, freq) instead of running it immediately,
// so callers started together (e.g. by a worker rollout) don't tick in lockstep.
// The regular cadence keeps that phase afterwards.
func WithIntervalJitter() IntervalOption {
	return func(o *intervalOptions) { o.jitter = true }
}

// Interval runs fn immediately (unless WithJitter), then every freq until ctx is done or the returned func is called.
func Interval(
	ctx context.Context,
	freq time.Duration,
	fn func(),
	opts ...IntervalOption,
) func() {
	var options intervalOptions
	for _, opt := range opts {
		opt(&options)
	}

	shutdown := make(chan struct{})
	go func() {
		if options.jitter {
			jitter := time.NewTimer(rand.N(freq)) //nolint:gosec // tick jitter, not security sensitive
			select {
			case <-shutdown:
				jitter.Stop()
				return
			case <-ctx.Done():
				jitter.Stop()
				return
			case <-jitter.C:
			}
		}

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
