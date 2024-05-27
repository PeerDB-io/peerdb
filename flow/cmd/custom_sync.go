package cmd

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"

	"crypto/sha256"

	"google.golang.org/grpc/metadata"
)

const peerdbPauseGuideDocLink = "https://docs.peerdb.io/features/pause-mirror"

func AuthenticateSyncRequest(ctx context.Context) error {
	var values []string
	var token string

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		values = md.Get("authorization")
	}

	if len(values) > 0 {
		token = values[0]
	}

	deploymentUid := peerdbenv.PeerDBDeploymentUID()
	hash := sha256.New()
	_, err := hash.Write([]byte(deploymentUid))
	if err != nil {
		slog.Error("Server error: unable to verify authorization", slog.Any("error", err))
		return errors.New("server error: unable to verify authorization. Please try again.")
	}

	deploymentUidHashed := hex.EncodeToString(hash.Sum(nil))
	if token != "Bearer "+deploymentUidHashed {
		slog.Error("Unauthorized: invalid authorization token", slog.String("token", token))
		return errors.New("unauthorized: invalid authorization token. Please check the token and try again.")
	}

	return nil
}

func (h *FlowRequestHandler) CustomSyncFlow(
	ctx context.Context, req *protos.CreateCustomFlowRequest,
) (*protos.CreateCustomFlowResponse, error) {
	err := AuthenticateSyncRequest(ctx)
	if err != nil {
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage:  err.Error(),
			Ok:            false,
		}, nil
	}

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
		slog.Error("Invalid sync number request",
			slog.Any("requested_number_of_syncs", req.NumberOfSyncs))
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
		slog.Error("Server error: unable to check if mirror exists", slog.Any("error", err))
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage:  "Server error: unable to check if mirror " + req.FlowJobName + " exists.",
			Ok:            false,
		}, nil
	}
	if !mirrorExists {
		slog.Error("Mirror does not exist", slog.Any("mirror_name", req.FlowJobName))
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
		slog.Error("Server error: unable to check the status of mirror",
			slog.Any("mirror", req.FlowJobName),
			slog.Any("error", mirrorStatusResponse.ErrorMessage))
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage: fmt.Sprintf("Server error: unable to check the status of mirror %s: %s",
				req.FlowJobName, mirrorStatusResponse.ErrorMessage),
			Ok: false,
		}, nil
	}

	if mirrorStatusResponse.CurrentFlowState != protos.FlowStatus_STATUS_PAUSED {
		slog.Error("Mirror is not paused", slog.Any("mirror", req.FlowJobName))
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
		return &protos.CreateCustomFlowResponse{
			FlowJobName:   req.FlowJobName,
			NumberOfSyncs: 0,
			ErrorMessage: fmt.Sprintf("Unable to kick off sync for mirror %s:%s",
				req.FlowJobName, err.Error()),
			Ok: false,
		}, nil
	}

	slog.Info("Custom sync started for mirror",
		slog.String("mirror", req.FlowJobName),
		slog.Int("number_of_syncs", int(req.NumberOfSyncs)))

	return &protos.CreateCustomFlowResponse{
		FlowJobName:   req.FlowJobName,
		NumberOfSyncs: req.NumberOfSyncs,
		ErrorMessage:  "",
		Ok:            true,
	}, nil
}
