package peerflow

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func GetSideEffect[T any](ctx workflow.Context, f func(workflow.Context) T) T {
	sideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return f(ctx)
	})

	var result T
	if err := sideEffect.Get(&result); err != nil {
		panic(err)
	}
	return result
}

func GetFlowMetadataContext(ctx workflow.Context, flowJobName string, sourceName string, destinationName string) (workflow.Context, error) {
	metadataCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	getMetadataFuture := workflow.ExecuteLocalActivity(metadataCtx, flowable.GetFlowMetadata, flowJobName, sourceName, destinationName)
	var metadata *protos.FlowContextMetadata
	if err := getMetadataFuture.Get(metadataCtx, &metadata); err != nil {
		return nil, err
	}
	return workflow.WithValue(ctx, shared.FlowMetadataKey, metadata), nil
}
