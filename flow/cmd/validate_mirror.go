package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

var CustomColumnTypeRegex = regexp.MustCompile(`^$|^[a-zA-Z][a-zA-Z0-9(),]*$`)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, req.ConnectionConfigs.FlowJobName)
	underMaintenance, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		slog.ErrorContext(ctx, "unable to check maintenance mode", slog.Any("error", err))
		return nil, exceptions.NewInternalApiError(fmt.Errorf("unable to load dynamic config: %w", err))
	}

	if underMaintenance {
		slog.WarnContext(ctx, "Validate request denied due to maintenance", "flowName", req.ConnectionConfigs.FlowJobName)
		return nil, exceptions.ErrUnderMaintenance
	}

	if !req.ConnectionConfigs.Resync {
		mirrorExists, existCheckErr := h.checkIfMirrorNameExists(ctx, req.ConnectionConfigs.FlowJobName)
		if existCheckErr != nil {
			slog.ErrorContext(ctx, "/validatecdc failed to check if mirror name exists", slog.Any("error", existCheckErr))
			return nil, exceptions.NewInternalApiError(fmt.Errorf("failed to check if mirror name exists: %w", existCheckErr))
		}

		if mirrorExists {
			return nil, exceptions.NewAlreadyExistsApiError(
				errors.New("mirror with name %s already exists: "+req.ConnectionConfigs.FlowJobName),
				exceptions.NewMirrorErrorInfo(map[string]string{
					exceptions.ErrorMetadataOffendingField: "flow_job_name",
				}))
		}
	}

	if req.ConnectionConfigs == nil {
		slog.ErrorContext(ctx, "/validatecdc connection configs is nil")
		return nil, exceptions.NewInvalidArgumentApiError(errors.New("connection configs is nil"))
	}

	if !req.ConnectionConfigs.DoInitialSnapshot && req.ConnectionConfigs.InitialSnapshotOnly {
		return nil, exceptions.NewInvalidArgumentApiError(errors.New("invalid config: initial_snapshot_only is true but do_initial_snapshot is false"))
	}

	for _, tm := range req.ConnectionConfigs.TableMappings {
		for _, col := range tm.Columns {
			if !CustomColumnTypeRegex.MatchString(col.DestinationType) {
				return nil, exceptions.NewInvalidArgumentApiError(errors.New("invalid custom column type " + col.DestinationType))
			}
		}
	}

	srcConn, err := connectors.GetByNameAs[connectors.MirrorSourceValidationConnector](
		ctx, req.ConnectionConfigs.Env, h.pool, req.ConnectionConfigs.SourceName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, exceptions.NewUnimplementedApiError(errors.New("connector is not a supported source type"))
		}
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Errorf("failed to create source connector: %s", err))
	}
	defer connectors.CloseConnector(ctx, srcConn)

	if err := srcConn.ValidateMirrorSource(ctx, req.ConnectionConfigs); err != nil {
		return nil, exceptions.NewFailedPreconditionApiError(
			fmt.Errorf("failed to validate source connector %s: %w", req.ConnectionConfigs.SourceName, err))
	}

	dstConn, err := connectors.GetByNameAs[connectors.MirrorDestinationValidationConnector](
		ctx, req.ConnectionConfigs.Env, h.pool, req.ConnectionConfigs.DestinationName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return &protos.ValidateCDCMirrorResponse{}, nil
		}
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Errorf("failed to create destination connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, dstConn)

	var tableSchemaMap map[string]*protos.TableSchema
	if !req.ConnectionConfigs.Resync {
		var getTableSchemaError error
		tableSchemaMap, getTableSchemaError = srcConn.GetTableSchema(ctx, req.ConnectionConfigs.Env, req.ConnectionConfigs.Version,
			req.ConnectionConfigs.System, req.ConnectionConfigs.TableMappings)
		if getTableSchemaError != nil {
			return nil, exceptions.NewFailedPreconditionApiError(fmt.Errorf("failed to get source table schema: %w", getTableSchemaError))
		}
	}
	if err := dstConn.ValidateMirrorDestination(ctx, req.ConnectionConfigs, tableSchemaMap); err != nil {
		return nil, exceptions.NewFailedPreconditionApiError(
			fmt.Errorf("failed to validate destination connector %s: %w", req.ConnectionConfigs.DestinationName, err))
	}

	return &protos.ValidateCDCMirrorResponse{}, nil
}

func (h *FlowRequestHandler) checkIfMirrorNameExists(ctx context.Context, mirrorName string) (bool, error) {
	var nameExists bool
	if err := h.pool.QueryRow(ctx, "SELECT EXISTS(SELECT * FROM flows WHERE name = $1)", mirrorName).Scan(&nameExists); err != nil {
		return false, fmt.Errorf("failed to check if mirror name exists: %w", err)
	}

	return nameExists, nil
}
