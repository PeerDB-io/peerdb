package utils

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

func HeartbeatRoutine(
	ctx context.Context,
	message func() string,
) func() {
	shutdown := make(chan struct{})
	go func() {
		counter := 0
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			counter += 1
			msg := fmt.Sprintf("heartbeat #%d: %s", counter, message())
			RecordHeartbeat(ctx, msg)
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

// if the functions are being called outside the context of a Temporal workflow,
// activity.RecordHeartbeat panics, this is a bandaid for that.
func RecordHeartbeat(ctx context.Context, details ...interface{}) {
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, details...)
	}
}
