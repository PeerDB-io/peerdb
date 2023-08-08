package utils

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

func HeartbeatRoutine(ctx context.Context, interval time.Duration) chan bool {
	counter := 1
	shutdownCh := make(chan bool)
	go func(shutdownCh chan bool) {
		for {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("heartbeat instance #%d for interval %v",
				counter, interval))
			counter += 1
			timeoutCh := time.After(interval)
			select {
			case <-shutdownCh:
				return
			case <-timeoutCh:
			}
		}
	}(shutdownCh)
	return shutdownCh
}
