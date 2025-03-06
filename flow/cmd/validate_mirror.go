package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/telemetry"
)

var (
	CustomColumnTypeRegex = regexp.MustCompile(`^$|^[a-zA-Z][a-zA-Z0-9(),]*$`)
	CustomColumnNameRegex = regexp.MustCompile(`^$|^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	underMaintenance, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		slog.Error("unable to check maintenance mode", slog.Any("error", err))
		return nil, fmt.Errorf("unable to load dynamic config: %w", err)
	}

	if underMaintenance {
		slog.Warn("Validate request denied due to maintenance", "flowName", req.ConnectionConfigs.FlowJobName)
		return nil, errors.New("PeerDB is under maintenance")
	}

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
		slog.Error("/validatecdc connection configs is nil")
		return nil, errors.New("connection configs is nil")
	}

	for _, tm := range req.ConnectionConfigs.TableMappings {
		for _, col := range tm.Columns {
			if !CustomColumnTypeRegex.MatchString(col.DestinationType) {
				return nil, fmt.Errorf("invalid custom column type %s", col.DestinationType)
			}
			if !CustomColumnNameRegex.MatchString(col.DestinationName) {
				return nil, fmt.Errorf("invalid custom column name %s", col.DestinationName)
			}
		}
	}

	srcConn, err := connectors.GetByNameAs[connectors.MirrorSourceValidationConnector](
		ctx, req.ConnectionConfigs.Env, h.pool, req.ConnectionConfigs.SourceName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, errors.New("/validatecdc source peer does not support being a source peer")
		}
		err := fmt.Errorf("failed to create source connector: %w", err)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			err.Error(),
		)
		return nil, err
	}
	defer connectors.CloseConnector(ctx, srcConn)

	if err := srcConn.ValidateMirrorSource(ctx, req.ConnectionConfigs); err != nil {
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			err.Error(),
		)
		return nil, err
	}

	dstConn, err := connectors.GetByNameAs[connectors.MirrorDestinationValidationConnector](
		ctx, req.ConnectionConfigs.Env, h.pool, req.ConnectionConfigs.DestinationName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return &protos.ValidateCDCMirrorResponse{}, nil
		}
		err := fmt.Errorf("failed to create destination connector: %w", err)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			err.Error(),
		)
		return nil, err
	}
	defer connectors.CloseConnector(ctx, dstConn)

	res, err := srcConn.GetTableSchema(ctx, nil, req.ConnectionConfigs.System, req.ConnectionConfigs.TableMappings)
	if err != nil {
		return nil, fmt.Errorf("failed to get source table schema: %w", err)
	}

	if err := dstConn.ValidateMirrorDestination(ctx, req.ConnectionConfigs, res); err != nil {
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
			err.Error(),
		)
		return nil, err
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
