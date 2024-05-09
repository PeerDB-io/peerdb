package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared/telemetry"
)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	if !req.ConnectionConfigs.Resync {
		mirrorExists, existCheckErr := h.CheckIfMirrorNameExists(ctx, req.ConnectionConfigs.FlowJobName)
		if existCheckErr != nil {
			slog.Error("/validatecdc failed to check if mirror name exists", slog.Any("error", existCheckErr))
			return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, existCheckErr
		}

		if mirrorExists {
			displayErr := fmt.Errorf("mirror with name %s already exists", req.ConnectionConfigs.FlowJobName)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
				fmt.Sprint(displayErr),
			)
			return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, displayErr
		}
	}

	if req.ConnectionConfigs == nil {
		slog.Error("/validatecdc connection configs is nil")
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, errors.New("connection configs is nil")
	}
	sourcePeerConfig := req.ConnectionConfigs.Source.GetPostgresConfig()
	if sourcePeerConfig == nil {
		slog.Error("/validatecdc source peer config is nil", slog.Any("peer", req.ConnectionConfigs.Source))
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, errors.New("source peer config is nil")
	}

	pgPeer, err := connpostgres.NewPostgresConnector(ctx, sourcePeerConfig)
	if err != nil {
		displayErr := fmt.Errorf("failed to create postgres connector: %v", err)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			fmt.Sprint(displayErr),
		)
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, displayErr
	}
	defer pgPeer.Close()

	// Check replication connectivity
	err = pgPeer.CheckReplicationConnectivity(ctx)
	if err != nil {
		displayErr := fmt.Errorf("unable to establish replication connectivity: %v", err)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			fmt.Sprint(displayErr),
		)
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, displayErr
	}

	// Check permissions of postgres peer
	err = pgPeer.CheckReplicationPermissions(ctx, sourcePeerConfig.User)
	if err != nil {
		displayErr := fmt.Errorf("failed to check replication permissions: %v", err)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			fmt.Sprint(displayErr),
		)
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, displayErr
	}

	// Check source tables
	sourceTables := make([]*utils.SchemaTable, 0, len(req.ConnectionConfigs.TableMappings))
	for _, tableMapping := range req.ConnectionConfigs.TableMappings {
		parsedTable, parseErr := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			displayErr := fmt.Errorf("invalid source table identifier: %s", parseErr)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
				fmt.Sprint(displayErr),
			)
			return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, displayErr
		}

		sourceTables = append(sourceTables, parsedTable)
	}

	pubName := req.ConnectionConfigs.PublicationName

	err = pgPeer.CheckSourceTables(ctx, sourceTables, pubName)
	if err != nil {
		displayErr := fmt.Errorf("provided source tables invalidated: %v", err)
		slog.Error(displayErr.Error())
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			fmt.Sprint(displayErr),
		)
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, displayErr
	}

	return &protos.ValidateCDCMirrorResponse{
		Ok: true,
	}, nil
}

func (h *FlowRequestHandler) CheckIfMirrorNameExists(ctx context.Context, mirrorName string) (bool, error) {
	var nameExists pgtype.Bool
	err := h.pool.QueryRow(ctx, "SELECT EXISTS(SELECT * FROM flows WHERE name = $1)", mirrorName).Scan(&nameExists)
	if err != nil {
		return true, fmt.Errorf("failed to check if mirror name exists: %v", err)
	}

	return nameExists.Bool, nil
}
