package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/mirrorutils"
	"github.com/PeerDB-io/peerdb/flow/shared/telemetry"
)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	if !req.ConnectionConfigs.Resync {
		mirrorExists, existCheckErr := h.CheckIfMirrorNameExists(ctx, req.ConnectionConfigs.FlowJobName)
		if existCheckErr != nil {
			slog.Error("/validatecdc failed to check if mirror name exists", slog.Any("error", existCheckErr))
			return nil, existCheckErr
		}

		if mirrorExists {
			displayErr := fmt.Errorf("mirror with name %s already exists", req.ConnectionConfigs.FlowJobName)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName, displayErr.Error())
			return nil, displayErr
		}
	}

	if req.ConnectionConfigs == nil {
		slog.Error("connection configs is nil")
		return nil, errors.New("connection configs is nil")
	}

	// once we add multiple source types, this can be changed to a switch based on source type
	// can then pass protos.Peer into this function
	if err := mirrorutils.PostgresSourceMirrorValidate(ctx, h.pool,
		h.alerter, req.ConnectionConfigs); err != nil {
		return nil, err
	}

	dstPeer, err := connectors.LoadPeer(ctx, h.pool, req.ConnectionConfigs.DestinationName)
	if err != nil {
		slog.Error("failed to load destination peer", slog.String("peer", req.ConnectionConfigs.DestinationName))
		return nil, err
	}
	if dstPeer.GetClickhouseConfig() != nil {
		if err := mirrorutils.ClickHouseDestinationMirrorValidate(ctx, h.pool,
			h.alerter, req.ConnectionConfigs); err != nil {
			return nil, err
		}
	}

	return &protos.ValidateCDCMirrorResponse{}, nil
}

func (h *FlowRequestHandler) CheckIfMirrorNameExists(ctx context.Context, mirrorName string) (bool, error) {
	var nameExists pgtype.Bool
	err := h.pool.QueryRow(ctx, "SELECT EXISTS(SELECT * FROM flows WHERE name = $1)", mirrorName).Scan(&nameExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if mirror name exists: %v", err)
	}

	return nameExists.Bool, nil
}
