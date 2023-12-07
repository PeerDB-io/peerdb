package logger

import (
	"context"
	"log/slog"
)

var _ slog.Handler = Handler{}

type ContextKey string

const (
	FlowName      ContextKey = "flowName"
	PartitionId   ContextKey = "partitionId"
	DeploymentUid ContextKey = "deploymentUid"
)

var fields = []ContextKey{FlowName, PartitionId, DeploymentUid}

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
	return h.handler.Handle(ctx, record)
}

func (h Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return Handler{h.handler.WithAttrs(attrs)}
}

func (h Handler) WithGroup(name string) slog.Handler {
	return h.handler.WithGroup(name)
}
