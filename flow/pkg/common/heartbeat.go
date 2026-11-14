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
