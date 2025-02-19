package shared

import (
	"context"
	"log/slog"
	"os"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

func LoggerFromCtx(ctx context.Context) log.Logger {
	var logger log.Logger

	if activity.IsActivity(ctx) {
		logger = activity.GetLogger(ctx)
	} else {
		logger = log.NewStructuredLogger(slog.Default())
	}

	flowName, hasName := ctx.Value(FlowNameKey).(string)
	if hasName {
		logger = log.With(logger, string(FlowNameKey), flowName)
	}

	if flowMetadata := internal.GetFlowMetadata(ctx); flowMetadata != nil {
		logger = log.With(logger, string(internal.FlowMetadataKey), flowMetadata)
	}

	return logger
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
