package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func (h *FlowRequestHandler) ValidatePeer(
	ctx context.Context,
	req *protos.ValidatePeerRequest,
) (*protos.ValidatePeerResponse, error) {
	if req.Peer == nil {
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: "no peer provided",
		}, nil
	}

	if req.Peer.Name == "" {
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: "no peer name provided",
		}, nil
	}

	conn, err := connectors.GetConnector(ctx, req.Peer)
	if err != nil {
		return &protos.ValidatePeerResponse{
			Status: protos.ValidatePeerStatus_INVALID,
			Message: fmt.Sprintf("%s peer %s was invalidated: %s",
				req.Peer.Type, req.Peer.Name, err),
		}, nil
	}

	defer conn.Close()

	if req.Peer.Type == protos.DBType_POSTGRES {
		isValid, version, err := conn.(*connpostgres.PostgresConnector).MajorVersionCheck(ctx, connpostgres.POSTGRES_12)
		if err != nil {
			slog.Error("/peer/validate: pg version check", slog.Any("error", err))
			return nil, err
		}

		if !isValid {
			return &protos.ValidatePeerResponse{
				Status: protos.ValidatePeerStatus_INVALID,
				Message: fmt.Sprintf("%s peer %s must be of version 12 or above. Current version: %d",
					req.Peer.Type, req.Peer.Name, version),
			}, nil
		}
	}

	connErr := conn.ConnectionActive(ctx)
	if connErr != nil {
		return &protos.ValidatePeerResponse{
			Status: protos.ValidatePeerStatus_INVALID,
			Message: fmt.Sprintf("failed to establish active connection to %s peer %s: %v",
				req.Peer.Type, req.Peer.Name, connErr),
		}, nil
	}

	return &protos.ValidatePeerResponse{
		Status: protos.ValidatePeerStatus_VALID,
		Message: fmt.Sprintf("%s peer %s is valid",
			req.Peer.Type, req.Peer.Name),
	}, nil
}
