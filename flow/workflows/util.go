package peerflow

import (
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
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
	metadataCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumInterval: time.Minute},
	})
	getMetadataFuture := workflow.ExecuteActivity(metadataCtx, flowable.GetFlowMetadata, input)
	var metadata *protos.FlowContextMetadata
	if err := getMetadataFuture.Get(metadataCtx, &metadata); err != nil {
		return ctx, err
	}
	return workflow.WithValue(ctx, internal.FlowMetadataKey, metadata), nil
}

func ShouldWorkflowContinueAsNew(ctx workflow.Context) bool {
	info := workflow.GetInfo(ctx)
	return info.GetContinueAsNewSuggested() &&
		(info.GetCurrentHistoryLength() > 40960 || info.GetCurrentHistorySize() > 40*1024*1024)
}

func getQRepOverwriteFullRefreshMode(wCtx workflow.Context, logger log.Logger, env map[string]string) bool {
	checkCtx := workflow.WithActivityOptions(wCtx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	var fullRefreshEnabled bool
	getFullRefreshFuture := workflow.ExecuteActivity(checkCtx, flowable.PeerDBFullRefreshOverwriteMode, env)
	if err := getFullRefreshFuture.Get(checkCtx, &fullRefreshEnabled); err != nil {
		logger.Warn("Failed to check if full refresh mode is enabled", slog.Any("error", err))
		return false
	}
	return fullRefreshEnabled
}
