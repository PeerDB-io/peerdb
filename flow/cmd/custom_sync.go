package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	peerdbPauseGuideDocLink string = "https://docs.peerdb.io/features/pause-mirror"
)

func (h *FlowRequestHandler) CustomSyncFlow(
	ctx context.Context, req *protos.CreateCustomSyncRequest,
) (*protos.CreateCustomSyncResponse, error) {
	if req.FlowJobName == "" {
		return nil, errors.New("mirror name cannot be empty")
	}

	if req.NumberOfSyncs <= 0 {
		slog.Error("Invalid sync number request",
			slog.Int64("requested_number_of_syncs", int64(req.NumberOfSyncs)))
		return nil, fmt.Errorf("sync number request must be greater than 0. Requested number: %d", req.NumberOfSyncs)
	}

	mirrorExists, err := h.CheckIfMirrorNameExists(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("Server error: unable to check if mirror exists", slog.Any("error", err))
		return nil, fmt.Errorf("server error: unable to check if mirror %s exists", req.FlowJobName)
	}
	if !mirrorExists {
		slog.Error("Mirror does not exist", slog.String("mirror_name", req.FlowJobName))
		return nil, fmt.Errorf("mirror %s does not exist", req.FlowJobName)
	}

	mirrorStatusResponse, err := h.MirrorStatus(ctx, &protos.MirrorStatusRequest{
		FlowJobName: req.FlowJobName,
	})
	if err != nil {
		slog.Error("Server error: unable to check the status of mirror",
			slog.String("mirror", req.FlowJobName), slog.Any("error", err))
		return nil, err
	}

	if mirrorStatusResponse.CurrentFlowState != protos.FlowStatus_STATUS_PAUSED {
		slog.Error("Mirror is not paused", slog.String("mirror", req.FlowJobName))
		return nil, fmt.Errorf(
			"requested mirror %s is not paused. This is a requirement. The mirror can be paused via PeerDB UI. Please follow %s",
			req.FlowJobName, peerdbPauseGuideDocLink)
	}

	// Resume mirror with custom sync number
	if _, err := h.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        req.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					NumberOfSyncs: req.NumberOfSyncs,
				},
			},
		},
	}); err != nil {
		slog.Error("Unable to kick off custom sync for mirror",
			slog.String("mirror", req.FlowJobName), slog.Any("error", err))
		return nil, fmt.Errorf("unable to kick off sync for mirror %s: %w", req.FlowJobName, err)
	}

	slog.Info("Custom sync started for mirror",
		slog.String("mirror", req.FlowJobName), slog.Int64("number_of_syncs", int64(req.NumberOfSyncs)))

	return &protos.CreateCustomSyncResponse{
		FlowJobName:   req.FlowJobName,
		NumberOfSyncs: req.NumberOfSyncs,
	}, nil
}
