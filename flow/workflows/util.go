package peerflow

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func GetSideEffect[T any](ctx workflow.Context, f func(workflow.Context) T) T {
	sideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return f(ctx)
	})

	var result T
	if err := sideEffect.Get(&result); err != nil {
		panic(err)
	}
	return result
}

func GetFlowMetadataContext(
	ctx workflow.Context,
	input *protos.FlowContextMetadataInput,
) (workflow.Context, error) {
	metadataCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	getMetadataFuture := workflow.ExecuteLocalActivity(metadataCtx, flowable.GetFlowMetadata, input)
	var metadata *protos.FlowContextMetadata
	if err := getMetadataFuture.Get(metadataCtx, &metadata); err != nil {
		return nil, err
	}
	return workflow.WithValue(ctx, internal.FlowMetadataKey, metadata), nil
}

func ShouldWorkflowContinueAsNew(ctx workflow.Context) bool {
	info := workflow.GetInfo(ctx)
	return info.GetContinueAsNewSuggested() &&
		(info.GetCurrentHistoryLength() > 40960 || info.GetCurrentHistorySize() > 40*1024*1024)
}
