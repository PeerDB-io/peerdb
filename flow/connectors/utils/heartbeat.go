package utils

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

func HeartbeatRoutine(
	ctx context.Context,
	interval time.Duration,
	message func() string,
) chan bool {
	counter := 1
	shutdown := make(chan bool)
	go func() {
		for {
			msg := fmt.Sprintf("heartbeat #%d: %s", counter, message())
			activity.RecordHeartbeat(ctx, msg)
			counter += 1
			to := time.After(interval)
			select {
			case <-shutdown:
				return
			case <-to:
			}
		}
	}()
	return shutdown
}
