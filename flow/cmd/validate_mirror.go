package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/proto_conversions"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

var CustomColumnTypeRegex = regexp.MustCompile(`^$|^[a-zA-Z][a-zA-Z0-9(),]*$`)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, APIError) {
	flowConnectionConfigsCore := proto_conversions.FlowConnectionConfigsToCore(req.ConnectionConfigs)
	return h.validateCDCMirrorImpl(ctx, flowConnectionConfigsCore, false)
}

func (h *FlowRequestHandler) validateCDCMirrorImpl(
	ctx context.Context, connectionConfigs *protos.FlowConnectionConfigsCore, idempotent bool,
) (*protos.ValidateCDCMirrorResponse, APIError) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, connectionConfigs.FlowJobName)
	underMaintenance, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		slog.ErrorContext(ctx, "unable to check maintenance mode", slog.Any("error", err))
		return nil, NewInternalApiError(fmt.Errorf("unable to load dynamic config: %w", err))
	}

	if underMaintenance {
		slog.WarnContext(ctx, "Validate request denied due to maintenance", "flowName", connectionConfigs.FlowJobName)
		return nil, NewUnavailableApiError(ErrUnderMaintenance)
	}

	// Skip mirror existence check when idempotent (for managed creates)
	if !idempotent && !connectionConfigs.Resync {
		mirrorExists, existCheckErr := h.checkIfMirrorNameExists(ctx, connectionConfigs.FlowJobName)
		if existCheckErr != nil {
			slog.ErrorContext(ctx, "/validatecdc failed to check if mirror name exists", slog.Any("error", existCheckErr))
			return nil, NewInternalApiError(fmt.Errorf("failed to check if mirror name exists: %w", existCheckErr))
		}

		if mirrorExists {
			return nil, NewAlreadyExistsApiError(
				errors.New("mirror with name %s already exists: "+connectionConfigs.FlowJobName),
				NewMirrorErrorInfo(map[string]string{
					ErrorMetadataOffendingField: "flow_job_name",
				}))
		}
	}

	if connectionConfigs == nil {
		slog.ErrorContext(ctx, "/validatecdc connection configs is nil")
		return nil, NewInvalidArgumentApiError(errors.New("connection configs is nil"))
	}

	if !connectionConfigs.DoInitialSnapshot && connectionConfigs.InitialSnapshotOnly {
		return nil, NewInvalidArgumentApiError(
			errors.New("invalid config: initial_snapshot_only is true but do_initial_snapshot is false"))
	}

	// fetch connection configs from DB
	tableMappings, err := internal.FetchTableMappingsFromDB(ctx, connectionConfigs.FlowJobName, connectionConfigs.TableMappingVersion)
	if err != nil {
		return nil, NewInternalApiError(err)
	}

	for _, tm := range tableMappings {
		for _, col := range tm.Columns {
			if !CustomColumnTypeRegex.MatchString(col.DestinationType) {
				return nil, NewInvalidArgumentApiError(errors.New("invalid custom column type " + col.DestinationType))
			}
		}
	}

	srcConn, srcClose, err := connectors.GetByNameAs[connectors.MirrorSourceValidationConnector](
		ctx, connectionConfigs.Env, h.pool, connectionConfigs.SourceName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, NewUnimplementedApiError(errors.New("connector is not a supported source type"))
		}
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to create source connector: %s", err))
	}
	defer srcClose(ctx)

	if err := srcConn.ValidateMirrorSource(ctx, connectionConfigs); err != nil {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("failed to validate source connector %s: %w", connectionConfigs.SourceName, err))
	}

	dstConn, dstClose, err := connectors.GetByNameAs[connectors.MirrorDestinationValidationConnector](
		ctx, connectionConfigs.Env, h.pool, connectionConfigs.DestinationName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return &protos.ValidateCDCMirrorResponse{}, nil
		}
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to create destination connector: %w", err))
	}
	defer dstClose(ctx)

	var tableSchemaMap map[string]*protos.TableSchema
	if !connectionConfigs.Resync {
		var getTableSchemaError error
		tableSchemaMap, getTableSchemaError = srcConn.GetTableSchema(ctx, connectionConfigs.Env, connectionConfigs.Version,
			connectionConfigs.System, tableMappings)
		if getTableSchemaError != nil {
			return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to get source table schema: %w", getTableSchemaError))
		}
	}
	if err := dstConn.ValidateMirrorDestination(ctx, connectionConfigs, tableSchemaMap); err != nil {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("failed to validate destination connector %s: %w", connectionConfigs.DestinationName, err))
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
