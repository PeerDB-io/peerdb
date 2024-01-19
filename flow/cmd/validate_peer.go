package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
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

	if len(req.Peer.Name) == 0 {
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: "no peer name provided",
		}, nil
	}

	if req.Peer.Type == protos.DBType_POSTGRES {
		slog.Info("validating postgres peer", slog.Any("peer", req.Peer))
		pgConfigStr := utils.GetPGConnectionString(req.Peer.GetPostgresConfig())
		if len(pgConfigStr) == 0 {
			return &protos.ValidatePeerResponse{
				Status:  protos.ValidatePeerStatus_INVALID,
				Message: "no postgres config provided",
			}, nil
		}

		peerConfig, err := pgxpool.ParseConfig(pgConfigStr)
		if err != nil {
			return &protos.ValidatePeerResponse{
				Status:  protos.ValidatePeerStatus_INVALID,
				Message: "invalid postgres config provided",
			}, nil
		}

		// Deny PG version < 12
		sourcePool, err := pgxpool.NewWithConfig(ctx, peerConfig)
		if err != nil {
			slog.Error("/peer/validate: failed to obtain peer connection", slog.Any("error", err))
			return nil, err
		}
		version, err := connpostgres.GetPostgresVersion(ctx, sourcePool)
		if err != nil {
			slog.Error("/peer/validate: pg version check", slog.Any("error", err))
			return nil, err
		}

		slog.Info("postgres version", slog.Any("version", version))
		if version < 12 {
			return &protos.ValidatePeerResponse{
				Status: protos.ValidatePeerStatus_INVALID,
				Message: fmt.Sprintf("%s peer %s must be of version 12 or above. Current version: %d",
					req.Peer.Type, req.Peer.Name, version),
			}, nil
		}
	}

	conn, err := connectors.GetConnector(ctx, req.Peer)
	if err != nil {
		return &protos.ValidatePeerResponse{
			Status: protos.ValidatePeerStatus_INVALID,
			Message: fmt.Sprintf("peer type is missing or "+
				"your requested configuration for %s peer %s was invalidated: %s",
				req.Peer.Type, req.Peer.Name, err),
		}, nil
	}

	connErr := conn.ConnectionActive()
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
