package common

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
	return HeartbeatRoutineEvery(ctx, 2*time.Second, message)
}

// HeartbeatRoutineEvery records a heartbeat every freq. The worker's MaxHeartbeatThrottleInterval
// caps how far apart heartbeat RPCs can be, so a freq above it sends one RPC per call, at freq.
func HeartbeatRoutineEvery(
	ctx context.Context,
	freq time.Duration,
	message func() string,
) func() {
	counter := 0
	return Interval(
		ctx,
		freq,
		func() {
			counter += 1
			activity.RecordHeartbeat(ctx, fmt.Sprintf("heartbeat #%d: %s", counter, message()))
		},
	)
}
