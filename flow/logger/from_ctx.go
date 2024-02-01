package logger

import (
	"context"
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/shared"
)

func LoggerFromCtx(ctx context.Context) log.Logger {
	flowName, hasName := ctx.Value(shared.FlowNameKey).(string)
	if activity.IsActivity(ctx) {
		if hasName {
			return log.With(activity.GetLogger(ctx), string(shared.FlowNameKey), flowName)
		} else {
			return activity.GetLogger(ctx)
		}
	} else if hasName {
		return slog.With(string(shared.FlowNameKey), flowName)
	} else {
		return slog.Default()
	}
}
