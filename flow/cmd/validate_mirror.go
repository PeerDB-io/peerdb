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
	pgPeer, err := connpostgres.NewPostgresConnector(ctx, req.ConnectionConfigs.Source.GetPostgresConfig())
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("failed to create postgres connector: %v", err)
	}

	defer pgPeer.Close()

	sourcePeerConfig := req.ConnectionConfigs.Source.GetPostgresConfig()
	if sourcePeerConfig == nil {
		slog.Error("/validatecdc source peer config is nil", slog.Any("peer", req.ConnectionConfigs.Source))
		return nil, fmt.Errorf("source peer config is nil")
	}

	// Check permissions of postgres peer
	err = pgPeer.CheckReplicationPermissions(sourcePeerConfig.User)
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

	err = pgPeer.CheckSourceTables(sourceTables, req.ConnectionConfigs.PublicationName)
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("provided source tables invalidated: %v", err)
	}

	return &protos.ValidateCDCMirrorResponse{
		Ok: true,
	}, nil
}
