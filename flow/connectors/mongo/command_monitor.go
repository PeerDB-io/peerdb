package connmongo

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.temporal.io/sdk/log"
)

type logCommandsKey struct{}

// withCommandLogging marks ctx so that commands sent with it are logged by the command monitor.
// The client is shared by every connector operation, and only change stream reads are of interest.
func withCommandLogging(ctx context.Context) context.Context {
	return context.WithValue(ctx, logCommandsKey{}, true)
}

func shouldLogCommand(ctx context.Context) bool {
	return ctx.Value(logCommandsKey{}) != nil
}

// newCommandMonitor logs command metadata for contexts marked by withCommandLogging. Only
// non-sensitive metadata should be logged.
func newCommandMonitor(logger log.Logger) *event.CommandMonitor {
	return &event.CommandMonitor{
		Succeeded: func(ctx context.Context, evt *event.CommandSucceededEvent) {
			if !shouldLogCommand(ctx) {
				return
			}
			logger.Info("[CommandMonitor] succeeded", maybeAppendBatchLen([]any{
				slog.String("command", evt.CommandName),
				slog.String("database", evt.DatabaseName),
				slog.String("connectionID", evt.ConnectionID),
				slog.Duration("duration", evt.Duration),
				slog.Int("size", len(evt.Reply)),
			}, evt.CommandName, evt.Reply)...)
		},
		Failed: func(ctx context.Context, evt *event.CommandFailedEvent) {
			if !shouldLogCommand(ctx) {
				return
			}
			logger.Info("[CommandMonitor] failed",
				slog.String("command", evt.CommandName),
				slog.String("database", evt.DatabaseName),
				slog.String("connectionID", evt.ConnectionID),
				slog.Duration("duration", evt.Duration),
				slog.Any("error", evt.Failure))
		},
	}
}

func maybeAppendBatchLen(attrs []any, command string, reply bson.Raw) []any {
	var batchField string
	switch command {
	case "getMore":
		batchField = "nextBatch"
	case "aggregate", "find":
		batchField = "firstBatch"
	default:
		return attrs
	}
	arr, found := reply.Lookup("cursor", batchField).ArrayOK()
	if !found {
		return attrs
	}
	if values, err := arr.Values(); err == nil {
		attrs = append(attrs, slog.Int("batchLen", len(values)))
	}
	return attrs
}

// newDebugCommandMonitor logs every command with its full reply, for local debugging only.
// Replies carry customer documents, so it must never be enabled in production.
func newDebugCommandMonitor(logger log.Logger) *event.CommandMonitor {
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
			var reply bson.M
			if err := bson.Unmarshal(evt.Reply, &reply); err == nil {
				logline += fmt.Sprintf(" reply=%+v", reply)
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
