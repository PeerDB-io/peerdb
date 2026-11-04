package peerflow

import (
	"fmt"
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

func GetSetupFlowHeartbeatTimeout(ctx workflow.Context) time.Duration {
	return time.Duration(GetSideEffect(ctx, func(workflow.Context) int {
		return internal.PeerDBSetupFlowHeartbeatTimeoutSeconds()
	})) * time.Second
}

func GetSetupFlowWorkflowTaskTimeout(ctx workflow.Context) time.Duration {
	return time.Duration(GetSideEffect(ctx, func(workflow.Context) int {
		return internal.PeerDBSetupFlowWorkflowTaskTimeoutSeconds()
	})) * time.Second
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

func getClickHouseInitialLoadAllowNonEmptyTables(wCtx workflow.Context, logger log.Logger, env map[string]string) bool {
	checkCtx := workflow.WithActivityOptions(wCtx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	var allow bool
	future := workflow.ExecuteActivity(checkCtx, flowable.PeerDBClickHouseInitialLoadAllowNonEmptyTables, env)
	if err := future.Get(checkCtx, &allow); err != nil {
		logger.Warn("Failed to check ClickHouse initial load allow non-empty tables", slog.Any("error", err))
		return false
	}
	return allow
}

// recordRenameTableUUIDs fills in the UUIDs of the tables to be renamed, so that a retried
// RenameTables can skip swaps that already happened instead of swapping the old tables back.
func recordRenameTableUUIDs(ctx workflow.Context, renameOpts *protos.RenameTablesInput) (*protos.RenameTablesInput, error) {
	recordCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Minute,
		},
	})
	var recordedOpts *protos.RenameTablesInput
	if err := workflow.ExecuteActivity(recordCtx, flowable.RecordRenameTableUUIDs, renameOpts).
		Get(recordCtx, &recordedOpts); err != nil {
		return nil, fmt.Errorf("failed to record table uuids for rename: %w", err)
	}
	return recordedOpts, nil
}
