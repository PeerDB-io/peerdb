package shared

import (
	"context"
	"fmt"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"go.temporal.io/sdk/client"
	"log/slog"
)

func GetWorkflowStatus(ctx context.Context, temporalClient client.Client, workflowID string) (protos.FlowStatus, error) {
	res, err := temporalClient.QueryWorkflow(ctx, workflowID, "", FlowStatusQuery)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to get status in workflow with ID %s: %s", workflowID, err.Error()))
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("failed to get status in workflow with ID %s: %w", workflowID, err)
	}
	var state protos.FlowStatus
	err = res.Get(&state)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to get status in workflow with ID %s: %s", workflowID, err.Error()))
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("failed to get status in workflow with ID %s: %w", workflowID, err)
	}
	return state, nil
}
