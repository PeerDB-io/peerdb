package common

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
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

func HeartbeatRoutine(
	ctx context.Context,
	message func() string,
) func() {
	counter := 0
	return Interval(
		ctx,
		15*time.Second,
		func() {
			counter += 1
			activity.RecordHeartbeat(ctx, fmt.Sprintf("heartbeat #%d: %s", counter, message()))
		},
	)
}
