package cmd

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

const peerdbPauseGuideDocLink = "https://docs.peerdb.io/features/pause-mirror"

func (h *FlowRequestHandler) CustomSyncFlow(
	ctx context.Context, req *protos.CreateCustomFlowRequest,
) (*protos.CreateCustomFlowResponse, error) {
	// ---- REQUEST VALIDATION ----
	if req.FlowJobName == "" {
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage:  "Flow job name is not provided",
			Ok:            false,
		}, nil
	}

	if req.NumberOfSyncs <= 0 || req.NumberOfSyncs > peerflow.MaxSyncsPerCdcFlow {
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage: fmt.Sprintf("Sync number request must be between 1 and %d (inclusive). Requested number: %d",
				peerflow.MaxSyncsPerCdcFlow, req.NumberOfSyncs),
			Ok: false,
		}, nil
	}

	mirrorExists, err := h.CheckIfMirrorNameExists(ctx, req.FlowJobName)
	if err != nil {
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage:  "Server error: unable to check if mirror " + req.FlowJobName + " exists.",
			Ok:            false,
		}, nil
	}
	if !mirrorExists {
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage:  req.FlowJobName + "does not exist. This may be because it was dropped.",
			Ok:            false,
		}, nil
	}

	mirrorStatusResponse, _ := h.MirrorStatus(ctx, &protos.MirrorStatusRequest{
		FlowJobName: req.FlowJobName,
	})
	if mirrorStatusResponse.ErrorMessage != "" {
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage: fmt.Sprintf("Server error: unable to check the status of mirror %s: %s",
				req.FlowJobName, mirrorStatusResponse.ErrorMessage),
			Ok: false,
		}, nil
	}

	if mirrorStatusResponse.CurrentFlowState != protos.FlowStatus_STATUS_PAUSED {
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage: fmt.Sprintf(`Requested mirror %s is not paused. This is a requirement.
            The mirror can be paused via PeerDB UI. Please follow %s`,
				req.FlowJobName, peerdbPauseGuideDocLink),
			Ok: false,
		}, nil
	}
	// ---- REQUEST VALIDATED ----

	// Resume mirror with custom sync number
	_, err = h.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:         req.FlowJobName,
		RequestedFlowState:  protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate:    nil,
		CustomNumberOfSyncs: req.NumberOfSyncs,
	})
	if err != nil {
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage: fmt.Sprintf("Unable to kick off sync for mirror %s:%s",
				req.FlowJobName, err.Error()),
			Ok: false,
		}, nil
	}

	return &protos.CreateCustomFlowResponse{
		FlowJobName:   req.FlowJobName,
		NumberOfSyncs: req.NumberOfSyncs,
		ErrorMessage:  "",
		Ok:            true,
	}, nil
}
