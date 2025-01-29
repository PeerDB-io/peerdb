package shared

import (
	"context"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type TemporalContextKey string

func (k TemporalContextKey) HeaderKey() string {
	return string(k)
}

const (
	FlowMetadataKey TemporalContextKey = "x-peerdb-flow-metadata"
)

type FlowMetadata struct {
	FlowName    string
	Source      PeerMetadata
	Destination PeerMetadata
}

type PeerMetadata struct {
	Name string
	Type protos.DBType
}

func GetFlowMetadata(ctx context.Context) *FlowMetadata {
	if metadata, ok := ctx.Value(FlowMetadataKey).(*FlowMetadata); ok {
		return metadata
	}
	return nil
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
