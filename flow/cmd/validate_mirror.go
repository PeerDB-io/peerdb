package main

import (
	"context"
	"fmt"
	"log/slog"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	sourcePeerName := req.ConnectionConfigs.Source.Name
	sourcePool, err := h.getPoolForPGPeer(ctx, sourcePeerName)
	if err != nil {
		slog.Error("/validatecdc failed to obtain peer connection", slog.Any("error", err))
		return nil, err
	}

	sourcePeerConfig := req.ConnectionConfigs.Source.GetPostgresConfig()
	if sourcePeerConfig == nil {
		slog.Error("/validatecdc source peer config is nil", slog.Any("peer", req.ConnectionConfigs.Source))
		return nil, fmt.Errorf("source peer config is nil")
	}

	// Check permissions of postgres peer
	err = connpostgres.CheckReplicationPermissions(ctx, sourcePool, sourcePeerConfig.User)
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("failed to check replication permissions: %v", err)
	}

	// Check source tables
	sourceTables := make([]string, 0, len(req.ConnectionConfigs.TableMappings))
	for _, tableMapping := range req.ConnectionConfigs.TableMappings {
		sourceTables = append(sourceTables, tableMapping.SourceTableIdentifier)
	}

	err = connpostgres.CheckSourceTables(ctx, sourcePool, sourceTables, req.ConnectionConfigs.PublicationName)
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("provided source tables invalidated: %v", err)
	}

	return &protos.ValidateCDCMirrorResponse{
		Ok: true,
	}, nil
}
