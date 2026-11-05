package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
)

// CancelTableResync signals the mirror to cancel its in-progress table-level resync. The mirror then
// resumes with the old table mappings. Fails once the resync started swapping tables. Cancellation is
// asynchronous: if swapping starts before the signal is handled, the resync runs to completion, or in a
// narrow race stays paused like a failed resync.
func (h *FlowRequestHandler) CancelTableResync(
	ctx context.Context,
	req *protos.CancelTableResyncRequest,
) (*protos.CancelTableResyncResponse, APIError) {
	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("unable to get workflow ID: %w", err))
	}
	state, err := h.getCDCWorkflowState(ctx, workflowID)
	if err != nil {
		return nil, NewInternalApiError(err)
	}
	// STATUS_RESYNC alone also matches a full mirror resync, so check for a pending table-level resync too
	if state.CurrentFlowStatus != protos.FlowStatus_STATUS_RESYNC ||
		state.FlowConfigUpdate == nil || len(state.FlowConfigUpdate.ResyncTables) == 0 {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("flow %s has no table-level resync in progress", req.FlowJobName))
	}
	if state.TableResyncSwapStarted {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("table resync of flow %s already started swapping tables and can no longer be cancelled", req.FlowJobName))
	}

	if err := model.CancelTableResyncSignal.SignalClientWorkflow(ctx, h.temporalClient, workflowID, "", struct{}{}); err != nil {
		return nil, NewInternalApiError(fmt.Errorf("unable to signal workflow: %w", err))
	}
	slog.InfoContext(ctx, "requested table resync cancellation",
		slog.String("flowJobName", req.FlowJobName), slog.String("workflowID", workflowID))
	return &protos.CancelTableResyncResponse{}, nil
}
