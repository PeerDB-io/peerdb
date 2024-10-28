package shared

import (
	"context"
	"log/slog"
	"os"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
)

func LoggerFromCtx(ctx context.Context) log.Logger {
	flowName, hasName := ctx.Value(FlowNameKey).(string)
	if activity.IsActivity(ctx) {
		if hasName {
			return log.With(activity.GetLogger(ctx), string(FlowNameKey), flowName)
		} else {
			return activity.GetLogger(ctx)
		}
	} else if hasName {
		return slog.With(string(FlowNameKey), flowName)
	} else {
		return slog.Default()
	}
}

func LogError(logger log.Logger, err error) error {
	logger.Error(err.Error())
	return err
}

var _ slog.Handler = SlogHandler{}

var fields = []ContextKey{FlowNameKey, PartitionIDKey}

type SlogHandler struct {
	slog.Handler
}

func NewSlogHandler(handler slog.Handler) slog.Handler {
	return SlogHandler{
		Handler: handler,
	}
}

func (h SlogHandler) Handle(ctx context.Context, record slog.Record) error {
	for _, field := range fields {
		if v, ok := ctx.Value(field).(string); ok {
			record.AddAttrs(slog.String(string(field), v))
		}
	}
	record.AddAttrs(slog.String(string(DeploymentUIDKey), os.Getenv("PEERDB_DEPLOYMENT_UID")))
	return h.Handler.Handle(ctx, record)
}
