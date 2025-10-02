package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (h *FlowRequestHandler) ValidatePeer(
	ctx context.Context,
	req *protos.ValidatePeerRequest,
) (*protos.ValidatePeerResponse, APIError) {
	if req.Peer == nil {
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: "no peer provided",
		}, NewInvalidArgumentApiError(errors.New("no peer provided"))
	}

	if req.Peer.Name == "" {
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: "no peer name provided",
		}, NewInvalidArgumentApiError(errors.New("no peer name provided"))
	}

	validatePeerDeadline := 15 * time.Second
	if req.Peer.Type == protos.DBType_CLICKHOUSE {
		// if instance is overloaded, DDL can take longer than 15s to execute
		validatePeerDeadline = 1 * time.Minute
	}

	ctx, cancelCtx := context.WithTimeout(ctx, validatePeerDeadline)
	defer cancelCtx()

	conn, err := connectors.GetConnector(ctx, nil, req.Peer)
	if err != nil {
		displayErr := fmt.Errorf("%s peer %s was invalidated: %w", req.Peer.Type, req.Peer.Name, err)
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: displayErr.Error(),
		}, NewFailedPreconditionApiError(displayErr)
	}
	defer conn.Close()

	if validationConn, ok := conn.(connectors.ValidationConnector); ok {
		if validErr := validationConn.ValidateCheck(ctx); validErr != nil {
			displayErr := fmt.Errorf("failed to validate peer %s: %w", req.Peer.Name, validErr)
			return &protos.ValidatePeerResponse{
				Status:  protos.ValidatePeerStatus_INVALID,
				Message: displayErr.Error(),
			}, NewFailedPreconditionApiError(displayErr)
		}
	}

	if connErr := conn.ConnectionActive(ctx); connErr != nil {
		displayErr := fmt.Errorf("failed to establish active connection to %s peer %s: %w", req.Peer.Type, req.Peer.Name, connErr)
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: displayErr.Error(),
		}, NewFailedPreconditionApiError(displayErr)
	}

	return &protos.ValidatePeerResponse{
		Status: protos.ValidatePeerStatus_VALID,
		Message: fmt.Sprintf("%s peer %s is valid",
			req.Peer.Type, req.Peer.Name),
	}, nil
}
