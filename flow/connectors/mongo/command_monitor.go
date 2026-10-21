package connmongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.temporal.io/sdk/log"
)

// NewCommandMonitor creates a command monitor for debugging MongoDB server/client communication.
func NewCommandMonitor(logger log.Logger) *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(ctx context.Context, evt *event.CommandStartedEvent) {
			logger.Debug(fmt.Sprintf("[CommandMonitor] started: command=%s database=%s requestID=%d",
				evt.CommandName,
				evt.DatabaseName,
				evt.RequestID,
			))
		},
		Succeeded: func(ctx context.Context, evt *event.CommandSucceededEvent) {
			logline := fmt.Sprintf("[CommandMonitor] succeeded: command=%s size=%d duration=%v",
				evt.CommandName, len(evt.Reply), evt.Duration)
			// never log the reply itself: aggregate/getMore replies carry customer documents
			if batchLen, ok := cursorBatchLen(evt.Reply); ok {
				logline += fmt.Sprintf(" batchLen=%d", batchLen)
			}
			logger.Debug(logline)
		},
		Failed: func(ctx context.Context, evt *event.CommandFailedEvent) {
			logger.Debug(fmt.Sprintf("[CommandMonitor] failed: command=%s duration=%v error=%v",
				evt.CommandName,
				evt.Duration,
				evt.Failure,
			))
		},
	}
}

// cursorBatchLen returns the number of documents in a cursor reply (cursor.firstBatch or cursor.nextBatch).
func cursorBatchLen(reply bson.Raw) (int, bool) {
	for _, key := range []string{"nextBatch", "firstBatch"} {
		val, err := reply.LookupErr("cursor", key)
		if err != nil {
			continue
		}
		arr, ok := val.ArrayOK()
		if !ok {
			return 0, false
		}
		values, err := arr.Values()
		if err != nil {
			return 0, false
		}
		return len(values), true
	}
	return 0, false
}
