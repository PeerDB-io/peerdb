package utils

import (
	"context"
	"fmt"
	"log/slog"
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
			RecordHeartbeatWithRecover(ctx, msg)
			select {
			case <-shutdown:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				ticker.Reset(15 * time.Second)
			}
		}
	}()
	return func() { close(shutdown) }
}

// if the functions are being called outside the context of a Temporal workflow,
// activity.RecordHeartbeat panics, this is a bandaid for that.
func RecordHeartbeatWithRecover(ctx context.Context, details ...interface{}) {
	defer func() {
		if r := recover(); r != nil {
			slog.Warn("ignoring panic from activity.RecordHeartbeat")
			slog.Warn("this can happen when function is invoked outside of a Temporal workflow")
		}
	}()
	activity.RecordHeartbeat(ctx, details...)
}
