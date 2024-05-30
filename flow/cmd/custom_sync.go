package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/metadata"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
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

	password := peerdbenv.PeerDBPassword()
	_, hashedKey, _ := strings.Cut(token, " ")
	if bcrypt.CompareHashAndPassword([]byte(hashedKey), []byte(password)) != nil {
		slog.Error("Unauthorized: invalid authorization token")
		return errors.New("unauthorized: invalid authorization token. Please check the token and try again")
	}

	return nil
}

func (h *FlowRequestHandler) CustomSyncFlow(
	ctx context.Context, req *protos.CreateCustomSyncRequest,
) (*protos.CreateCustomSyncResponse, error) {
	errResponse := &protos.CreateCustomSyncResponse{
		FlowJobName:   req.FlowJobName,
		NumberOfSyncs: 0,
		ErrorMessage:  "error while processing request",
		Ok:            false,
	}
	err := AuthenticateSyncRequest(ctx)
	if err != nil {
		errResponse.ErrorMessage = err.Error()
		return errResponse, nil
	}

	// ---- REQUEST VALIDATION ----
	if req.FlowJobName == "" {
		errResponse.ErrorMessage = "Mirror name cannot be empty."
		return errResponse, nil
	}

	if req.NumberOfSyncs <= 0 || req.NumberOfSyncs > peerflow.MaxSyncsPerCdcFlow {
		slog.Error("Invalid sync number request",
			slog.Any("requested_number_of_syncs", req.NumberOfSyncs))
		errResponse.ErrorMessage = fmt.Sprintf("Sync number request must be between 1 and %d (inclusive). Requested number: %d",
			peerflow.MaxSyncsPerCdcFlow, req.NumberOfSyncs)
		return errResponse, nil
	}

	mirrorExists, err := h.CheckIfMirrorNameExists(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("Server error: unable to check if mirror exists", slog.Any("error", err))
		errResponse.ErrorMessage = "Server error: unable to check if mirror " + req.FlowJobName + " exists."
		return errResponse, nil
	}
	if !mirrorExists {
		slog.Error("Mirror does not exist", slog.Any("mirror_name", req.FlowJobName))
		errResponse.ErrorMessage = fmt.Sprintf("Mirror %s does not exist", req.FlowJobName)
		return errResponse, nil
	}

	mirrorStatusResponse, _ := h.MirrorStatus(ctx, &protos.MirrorStatusRequest{
		FlowJobName: req.FlowJobName,
	})
	if mirrorStatusResponse.ErrorMessage != "" {
		slog.Error("Server error: unable to check the status of mirror",
			slog.Any("mirror", req.FlowJobName),
			slog.Any("error", mirrorStatusResponse.ErrorMessage))
		errResponse.ErrorMessage = fmt.Sprintf("Server error: unable to check the status of mirror %s: %s",
			req.FlowJobName, mirrorStatusResponse.ErrorMessage)
		return errResponse, nil
	}

	if mirrorStatusResponse.CurrentFlowState != protos.FlowStatus_STATUS_PAUSED {
		slog.Error("Mirror is not paused", slog.Any("mirror", req.FlowJobName))
		errResponse.ErrorMessage = fmt.Sprintf(`Requested mirror %s is not paused. This is a requirement.
		The mirror can be paused via PeerDB UI. Please follow %s`,
			req.FlowJobName, peerdbPauseGuideDocLink)
		return errResponse, nil
	}

	// Parallel sync-normalise should not be enabled
	parallelSyncNormaliseEnabled := peerdbenv.PeerDBEnableParallelSyncNormalize()
	if parallelSyncNormaliseEnabled {
		errResponse.ErrorMessage = "Parallel sync-normalise is enabled. Please contact PeerDB support to disable it to proceed."
		return errResponse, nil
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
		errResponse.ErrorMessage = fmt.Sprintf("Unable to kick off sync for mirror %s:%s",
			req.FlowJobName, err.Error())
		return errResponse, nil
	}

	slog.Info("Custom sync started for mirror",
		slog.String("mirror", req.FlowJobName),
		slog.Int("number_of_syncs", int(req.NumberOfSyncs)))

	return &protos.CreateCustomSyncResponse{
		FlowJobName:   req.FlowJobName,
		NumberOfSyncs: req.NumberOfSyncs,
		ErrorMessage:  "",
		Ok:            true,
	}, nil
}
