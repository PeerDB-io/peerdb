package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

const (
	peerdbPauseGuideDocLink string = "https://docs.peerdb.io/features/pause-mirror"
	syncRequestLimit        int32  = 32
)

func (h *FlowRequestHandler) CustomSyncFlow(
	ctx context.Context, req *protos.CreateCustomSyncRequest,
) (*protos.CreateCustomSyncResponse, error) {
	// ---- REQUEST VALIDATION ----
	if req.FlowJobName == "" {
		return nil, errors.New("mirror name cannot be empty")
	}

	if req.NumberOfSyncs <= 0 || req.NumberOfSyncs > syncRequestLimit {
		slog.Error("Invalid sync number request",
			slog.Any("requested_number_of_syncs", req.NumberOfSyncs))
		return nil, fmt.Errorf("sync number request must be between 1 and %d (inclusive). Requested number: %d",
			syncRequestLimit, req.NumberOfSyncs)
	}

	mirrorExists, err := h.CheckIfMirrorNameExists(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("Server error: unable to check if mirror exists", slog.Any("error", err))
		return nil, fmt.Errorf("server error: unable to check if mirror %s exists", req.FlowJobName)
	}
	if !mirrorExists {
		slog.Error("Mirror does not exist", slog.Any("mirror_name", req.FlowJobName))
		return nil, fmt.Errorf("mirror %s does not exist", req.FlowJobName)
	}

	mirrorStatusResponse, err := h.MirrorStatus(ctx, &protos.MirrorStatusRequest{
		FlowJobName: req.FlowJobName,
	})
	if err != nil {
		slog.Error("Server error: unable to check the status of mirror",
			slog.String("mirror", req.FlowJobName),
			slog.Any("error", err))
		return nil, err
	}

	if mirrorStatusResponse.CurrentFlowState != protos.FlowStatus_STATUS_PAUSED {
		slog.Error("Mirror is not paused", slog.Any("mirror", req.FlowJobName))
		return nil, fmt.Errorf(
			"requested mirror %s is not paused. This is a requirement. The mirror can be paused via PeerDB UI. Please follow %s",
			req.FlowJobName, peerdbPauseGuideDocLink)
	}

	// Parallel sync-normalise should not be enabled
	parallelSyncNormaliseEnabled, err := peerdbenv.PeerDBEnableParallelSyncNormalize(ctx, nil)
	if err != nil {
		return nil, errors.New("server error: unable to check if parallel sync-normalise is enabled")
	}
	if parallelSyncNormaliseEnabled {
		return nil, errors.New("parallel sync-normalise is enabled. Please contact PeerDB support to disable it to proceed")
	}
	// ---- REQUEST VALIDATED ----

	// Resume mirror with custom sync number
	_, err = h.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        req.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					NumberOfSyncs: req.NumberOfSyncs,
				},
			},
		},
	})
	if err != nil {
		slog.Error("Unable to kick off custom sync for mirror",
			slog.Any("mirror", req.FlowJobName),
			slog.Any("error", err))
		return nil, fmt.Errorf("unable to kick off sync for mirror %s: %w", req.FlowJobName, err)
	}

	slog.Info("Custom sync started for mirror",
		slog.String("mirror", req.FlowJobName),
		slog.Int("number_of_syncs", int(req.NumberOfSyncs)))

	return &protos.CreateCustomSyncResponse{
		FlowJobName:   req.FlowJobName,
		NumberOfSyncs: req.NumberOfSyncs,
	}, nil
}
