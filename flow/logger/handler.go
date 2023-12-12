package logger

import (
	"context"
	"log/slog"
	"os"

	"github.com/PeerDB-io/peer-flow/shared"
)

var _ slog.Handler = Handler{}

var fields = []shared.ContextKey{shared.FlowNameKey, shared.PartitionIDKey}

type Handler struct {
	handler slog.Handler
}

func NewHandler(handler slog.Handler) slog.Handler {
	return Handler{
		handler: handler,
	}
}

func (h Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h Handler) Handle(ctx context.Context, record slog.Record) error {
	for _, field := range fields {
		if v, ok := ctx.Value(field).(string); ok {
			record.AddAttrs(slog.String(string(field), v))
		}
	}
	record.AddAttrs(slog.String(string(shared.DeploymentUIDKey), os.Getenv("PEERDB_DEPLOYMENT_UID")))
	return h.handler.Handle(ctx, record)
}

func (h Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return Handler{h.handler.WithAttrs(attrs)}
}

func (h Handler) WithGroup(name string) slog.Handler {
	return h.handler.WithGroup(name)
}
