package utils

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

func HeartbeatRoutine(ctx context.Context, interval time.Duration) chan bool {
	counter := 1
	shutdown := make(chan bool)
	go func() {
		for {
			msg := fmt.Sprintf("heartbeat instance #%d for interval %v", counter, interval)
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
