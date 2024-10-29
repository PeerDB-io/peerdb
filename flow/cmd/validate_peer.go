package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/PeerDB-io/peer-flow/shared/telemetry"
)

func (h *FlowRequestHandler) ValidatePeer(
	ctx context.Context,
	req *protos.ValidatePeerRequest,
) (*protos.ValidatePeerResponse, error) {
	ctx, cancelCtx := context.WithTimeout(ctx, time.Minute)
	defer cancelCtx()
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

	conn, err := connectors.GetConnector(ctx, nil, req.Peer)
	if err != nil {
		displayErr := fmt.Sprintf("%s peer %s was invalidated: %v", req.Peer.Type, req.Peer.Name, err)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreatePeer, req.Peer.Name, displayErr)
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: displayErr,
		}, nil
	}
	defer conn.Close()

	if req.Peer.Type == protos.DBType_POSTGRES {
		pgversion, err := conn.(*connpostgres.PostgresConnector).MajorVersion(ctx)
		if err != nil {
			slog.Error("/peer/validate: pg version check", slog.Any("error", err))
			return nil, err
		}

		if pgversion < shared.POSTGRES_12 {
			return &protos.ValidatePeerResponse{
				Status: protos.ValidatePeerStatus_INVALID,
				Message: fmt.Sprintf("Postgres peer %s must be of PG12 or above. Current version: %d",
					req.Peer.Name, pgversion),
			}, nil
		}
	}

	if validationConn, ok := conn.(connectors.ValidationConnector); ok {
		if validErr := validationConn.ValidateCheck(ctx); validErr != nil {
			displayErr := fmt.Sprintf("failed to validate peer %s: %v", req.Peer.Name, validErr)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreatePeer, req.Peer.Name,
				displayErr,
			)
			return &protos.ValidatePeerResponse{
				Status:  protos.ValidatePeerStatus_INVALID,
				Message: displayErr,
			}, nil
		}
	}

	if connErr := conn.ConnectionActive(ctx); connErr != nil {
		displayErr := fmt.Sprintf("failed to establish active connection to %s peer %s: %v", req.Peer.Type, req.Peer.Name, connErr)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreatePeer, req.Peer.Name,
			displayErr,
		)
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: displayErr,
		}, nil
	}

	return &protos.ValidatePeerResponse{
		Status: protos.ValidatePeerStatus_VALID,
		Message: fmt.Sprintf("%s peer %s is valid",
			req.Peer.Type, req.Peer.Name),
	}, nil
}
