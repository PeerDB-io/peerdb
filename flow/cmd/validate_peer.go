package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/telemetry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *FlowRequestHandler) ValidatePeer(
	ctx context.Context,
	req *protos.ValidatePeerRequest,
) (*protos.ValidatePeerResponse, error) {
	ctx, cancelCtx := context.WithTimeout(ctx, time.Minute)
	defer cancelCtx()
	if req.Peer == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no peer provided")
	}

	if req.Peer.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "no peer name provided")
	}

	conn, err := connectors.GetConnector(ctx, nil, req.Peer)
	if err != nil {
		displayErr := fmt.Sprintf("%s peer %s was invalidated: %v", req.Peer.Type, req.Peer.Name, err)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreatePeer, req.Peer.Name, displayErr)
		// Consider codes.Internal if it's an unexpected system issue, 
		// or codes.Unavailable if a dependent service is temporarily down.
		return nil, status.Errorf(codes.Internal, displayErr)
	}
	defer conn.Close()

	if validationConn, ok := conn.(connectors.ValidationConnector); ok {
		if validErr := validationConn.ValidateCheck(ctx); validErr != nil {
			displayErr := fmt.Sprintf("failed to validate peer %s: %v", req.Peer.Name, validErr)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreatePeer, req.Peer.Name,
				displayErr,
			)
			// codes.FailedPrecondition if the peer's state is not suitable for the operation
			// codes.InvalidArgument if the peer's configuration itself is invalid
			return nil, status.Errorf(codes.FailedPrecondition, displayErr)
		}
	}

	if connErr := conn.ConnectionActive(ctx); connErr != nil {
		displayErr := fmt.Sprintf("failed to establish active connection to %s peer %s: %v", req.Peer.Type, req.Peer.Name, connErr)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreatePeer, req.Peer.Name,
			displayErr,
		)
		// codes.Unavailable if the peer is temporarily unreachable
		// codes.FailedPrecondition if there's a persistent issue preventing connection
		return nil, status.Errorf(codes.Unavailable, displayErr)
	}

	return &protos.ValidatePeerResponse{
		Status: protos.ValidatePeerStatus_VALID,
		Message: fmt.Sprintf("%s peer %s is valid",
			req.Peer.Type, req.Peer.Name),
	}, nil
}
