package cmd

import (
	"context"
	"fmt"
	"log/slog"

	tEnums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

func (h *FlowRequestHandler) CancelTableAddition(
	ctx context.Context,
	req *protos.CancelTableAdditionInput,
) (*protos.CancelTableAdditionOutput, APIError) {
	flowStatus, err := internal.GetWorkflowStatusByName(ctx, h.pool, req.FlowJobName)
	if err != nil {
		return nil, NewInternalApiError(
			fmt.Errorf("failed to get workflow status for flow %s: %w", req.FlowJobName, err))
	}
	if flowStatus != protos.FlowStatus_STATUS_SETUP &&
		flowStatus != protos.FlowStatus_STATUS_SNAPSHOT {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("cannot cancel table addition for flow %s in status %s", req.FlowJobName, flowStatus.String()))
	}

	workflowID := fmt.Sprintf("table-addition-cancellation-%s-%s", req.FlowJobName, req.IdempotencyKey)

	// Start the cancel table addition workflow
	workflowOptions := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                string(shared.PeerFlowTaskQueue),
		TypedSearchAttributes:    shared.NewSearchAttributes(req.FlowJobName),
		WorkflowIDConflictPolicy: tEnums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    tEnums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}

	cancelTableAdditionFuture, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, peerflow.CancelTableAdditionFlow, req)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to start cancel table addition workflow: %w", err))
	}

	var output *protos.CancelTableAdditionOutput
	err = cancelTableAdditionFuture.Get(ctx, &output)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("cancel table addition workflow failed: %w", err))
	}

	slog.InfoContext(ctx, "Started cancel table addition workflow",
		slog.String("flowJobName", req.FlowJobName),
		slog.String("workflowID", workflowID))

	return output, nil
}
