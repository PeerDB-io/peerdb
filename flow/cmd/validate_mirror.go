package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	if req.ConnectionConfigs == nil {
		slog.Error("/validatecdc connection configs is nil")
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("connection configs is nil")
	}
	sourcePeerConfig := req.ConnectionConfigs.Source.GetPostgresConfig()
	if sourcePeerConfig == nil {
		slog.Error("/validatecdc source peer config is nil", slog.Any("peer", req.ConnectionConfigs.Source))
		return nil, fmt.Errorf("source peer config is nil")
	}

	pgPeer, err := connpostgres.NewPostgresConnector(ctx, sourcePeerConfig)
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("failed to create postgres connector: %v", err)
	}
	defer pgPeer.Close(ctx)

	// Check replication connectivity
	err = pgPeer.CheckReplicationConnectivity(ctx)
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("unable to establish replication connectivity: %v", err)
	}

	// Check permissions of postgres peer
	err = pgPeer.CheckReplicationPermissions(ctx, sourcePeerConfig.User)
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("failed to check replication permissions: %v", err)
	}

	// Check source tables
	sourceTables := make([]*utils.SchemaTable, 0, len(req.ConnectionConfigs.TableMappings))
	for _, tableMapping := range req.ConnectionConfigs.TableMappings {
		parsedTable, parseErr := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, fmt.Errorf("invalid source table identifier: %s", tableMapping.SourceTableIdentifier)
		}

		sourceTables = append(sourceTables, parsedTable)
	}

	pubName := req.ConnectionConfigs.PublicationName
	if pubName == "" {
		pubTables := make([]string, 0, len(sourceTables))
		for _, table := range sourceTables {
			pubTables = append(pubTables, table.String())
		}
		pubTableStr := strings.Join(pubTables, ", ")
		pubErr := pgPeer.CheckPublicationPermission(ctx, pubTableStr)
		if pubErr != nil {
			return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, fmt.Errorf("failed to check publication permission: %v", pubErr)
		}
	} else {
		err = pgPeer.CheckSourceTables(ctx, sourceTables, req.ConnectionConfigs.PublicationName)
		if err != nil {
			return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, fmt.Errorf("provided source tables invalidated: %v", err)
		}
	}

	return &protos.ValidateCDCMirrorResponse{
		Ok: true,
	}, nil
}
