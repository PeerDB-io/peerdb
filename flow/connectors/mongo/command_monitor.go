package connmongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.temporal.io/sdk/log"
)

// NewCommandMonitor creates a command monitor for debugging MongoDB operations.
// To enable, set the environment variable MONGO_COMMAND_MONITOR=true
func NewCommandMonitor(logger log.Logger) *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(ctx context.Context, evt *event.CommandStartedEvent) {
			logger.Info(fmt.Sprintf("[CommandMonitor] started: command=%s database=%s requestID=%d",
				evt.CommandName,
				evt.DatabaseName,
				evt.RequestID,
			))
		},
		Succeeded: func(ctx context.Context, evt *event.CommandSucceededEvent) {
			logline := fmt.Sprintf("[CommandMonitor] succeeded: command=%s size=%d duration=%v",
				evt.CommandName, len(evt.Reply), evt.Duration)
			var reply bson.M
			if err := bson.Unmarshal(evt.Reply, &reply); err == nil {
				logline += fmt.Sprintf(" reply=%+v", reply)
			}
			logger.Info(logline)
		},
		Failed: func(ctx context.Context, evt *event.CommandFailedEvent) {
			logger.Warn(fmt.Sprintf("[CommandMonitor] failed: command=%s duration=%v error=%v",
				evt.CommandName,
				evt.Duration,
				evt.Failure,
			))
		},
	}
}
