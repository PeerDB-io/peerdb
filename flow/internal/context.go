package internal

import (
	"context"
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type TemporalContextKey string

func (k TemporalContextKey) HeaderKey() string {
	return string(k)
}

const (
	FlowMetadataKey       TemporalContextKey = "x-peerdb-flow-metadata"
	AdditionalMetadataKey TemporalContextKey = "x-peerdb-additional-metadata"
)

// AdditionalContextMetadata has contextual information of a flow and is specific to a child context and is passed by copy
type AdditionalContextMetadata struct {
	Operation protos.FlowOperation
}

func GetFlowMetadata(ctx context.Context) *protos.FlowContextMetadata {
	if metadata, ok := ctx.Value(FlowMetadataKey).(*protos.FlowContextMetadata); ok {
		return metadata
	}
	return nil
}

func GetAdditionalMetadata(ctx context.Context) AdditionalContextMetadata {
	metadata, _ := ctx.Value(AdditionalMetadataKey).(AdditionalContextMetadata)
	return metadata
}

func WithOperationContext(ctx context.Context, operation protos.FlowOperation) context.Context {
	currentMetadata := GetAdditionalMetadata(ctx)
	currentMetadata.Operation = operation
	return context.WithValue(ctx, AdditionalMetadataKey, currentMetadata)
}

type ContextPropagator[V any] struct {
	Key TemporalContextKey
}

func NewContextPropagator[V any](key TemporalContextKey) workflow.ContextPropagator {
	return &ContextPropagator[V]{Key: key}
}

func (c *ContextPropagator[V]) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	value := ctx.Value(c.Key)
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		return err
	}
	writer.Set(c.Key.HeaderKey(), payload)
	return nil
}

func (c *ContextPropagator[V]) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	if payload, ok := reader.Get(c.Key.HeaderKey()); ok {
		var value V
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &value); err != nil {
			return ctx, nil
		}
		ctx = context.WithValue(ctx, c.Key, value)
	}

	return ctx, nil
}

func (c *ContextPropagator[V]) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	value := ctx.Value(c.Key)
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		return err
	}
	writer.Set(c.Key.HeaderKey(), payload)
	return nil
}

func (c *ContextPropagator[V]) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	if payload, ok := reader.Get(c.Key.HeaderKey()); ok {
		var value V
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &value); err != nil {
			return ctx, nil
		}
		ctx = workflow.WithValue(ctx, c.Key, value)
	}

	return ctx, nil
}

func LoggerFromCtx(ctx context.Context) log.Logger {
	var logger log.Logger

	if activity.IsActivity(ctx) {
		logger = activity.GetLogger(ctx)
	} else {
		logger = log.NewStructuredLogger(slog.Default())
	}

	flowName, hasName := ctx.Value(shared.FlowNameKey).(string)
	if hasName {
		logger = log.With(logger, string(shared.FlowNameKey), flowName)
	}

	if flowMetadata := GetFlowMetadata(ctx); flowMetadata != nil {
		logger = log.With(logger, string(FlowMetadataKey), flowMetadata)
	}
	logger = log.With(logger, string(AdditionalMetadataKey), GetAdditionalMetadata(ctx))

	return logger
}
