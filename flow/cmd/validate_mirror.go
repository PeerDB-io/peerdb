package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared/telemetry"
)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	if req.ConnectionConfigs == nil {
		slog.Error("/validatecdc connection configs is nil")
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, errors.New("connection configs is nil")
	}
	sourcePeerConfig := req.ConnectionConfigs.Source.GetPostgresConfig()
	if sourcePeerConfig == nil {
		slog.Error("/validatecdc source peer config is nil", slog.Any("peer", req.ConnectionConfigs.Source))
		return nil, errors.New("source peer config is nil")
	}

	pgPeer, err := connpostgres.NewPostgresConnector(ctx, sourcePeerConfig)
	if err != nil {
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			fmt.Sprintf("failed to create postgres connector: %v", err),
		)
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("failed to create postgres connector: %v", err)
	}
	defer pgPeer.Close()

	// Check replication connectivity
	err = pgPeer.CheckReplicationConnectivity(ctx)
	if err != nil {
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			fmt.Sprintf("unable to establish replication connectivity: %v", err),
		)
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("unable to establish replication connectivity: %v", err)
	}

	// Check permissions of postgres peer
	err = pgPeer.CheckReplicationPermissions(ctx, sourcePeerConfig.User)
	if err != nil {
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			fmt.Sprintf("failed to check replication permissions: %v", err),
		)
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("failed to check replication permissions: %v", err)
	}

	// Check source tables
	sourceTables := make([]*utils.SchemaTable, 0, len(req.ConnectionConfigs.TableMappings))
	for _, tableMapping := range req.ConnectionConfigs.TableMappings {
		parsedTable, parseErr := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
				"invalid source table identifier: "+tableMapping.SourceTableIdentifier,
			)
			return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, fmt.Errorf("invalid source table identifier: %s", tableMapping.SourceTableIdentifier)
		}

		sourceTables = append(sourceTables, parsedTable)
	}

	pubName := req.ConnectionConfigs.PublicationName
	if pubName != "" {
		err = pgPeer.CheckSourceTables(ctx, sourceTables, pubName)
		if err != nil {
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
				fmt.Sprintf("provided source tables invalidated: %v", err),
			)
			return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, fmt.Errorf("provided source tables invalidated: %v", err)
		}
	}

	return &protos.ValidateCDCMirrorResponse{
		Ok: true,
	}, nil
}
