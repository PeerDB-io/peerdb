package shared

import (
	"context"
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func GetWorkflowStatus(ctx context.Context, temporalClient client.Client, workflowID string) (protos.FlowStatus, error) {
	res, err := temporalClient.QueryWorkflow(ctx, workflowID, "", FlowStatusQuery)
	if err != nil {
		slog.Error("failed to query status in workflow with ID "+workflowID, slog.Any("error", err))
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("failed to query status in workflow with ID %s: %w", workflowID, err)
	}
	var state protos.FlowStatus
	if err := res.Get(&state); err != nil {
		slog.Error("failed to get status in workflow with ID "+workflowID, slog.Any("error", err))
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("failed to get status in workflow with ID %s: %w", workflowID, err)
	}
	return state, nil
}

func ShouldWorkflowContinueAsNew(ctx workflow.Context) bool {
	info := workflow.GetInfo(ctx)
	return info.GetContinueAsNewSuggested() &&
		(info.GetCurrentHistoryLength() > 40960 || info.GetCurrentHistorySize() > 40*1024*1024)
}
