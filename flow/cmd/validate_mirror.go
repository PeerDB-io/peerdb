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
		slog.Error("unable to check maintenance mode", slog.Any("error", err))
		return nil, fmt.Errorf("unable to load dynamic config: %w", err)
	}

	if underMaintenance {
		slog.Warn("Validate request denied due to maintenance", "flowName", req.ConnectionConfigs.FlowJobName)
		return nil, exceptions.ErrUnderMaintenance
	}

	if !req.ConnectionConfigs.Resync {
		mirrorExists, existCheckErr := h.CheckIfMirrorNameExists(ctx, req.ConnectionConfigs.FlowJobName)
		if existCheckErr != nil {
			slog.Error("/validatecdc failed to check if mirror name exists", slog.Any("error", existCheckErr))
			return nil, existCheckErr
		}

		if mirrorExists {
			return nil, fmt.Errorf("mirror with name %s already exists", req.ConnectionConfigs.FlowJobName)
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
		}
	}

	srcConn, err := connectors.GetByNameAs[connectors.MirrorSourceValidationConnector](
		ctx, req.ConnectionConfigs.Env, h.pool, req.ConnectionConfigs.SourceName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, errors.New("connector is not a supported source type")
		}
		return nil, fmt.Errorf("failed to create source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	if err := srcConn.ValidateMirrorSource(ctx, req.ConnectionConfigs); err != nil {
		return nil, fmt.Errorf("failed to validate source connector %s: %w", req.ConnectionConfigs.SourceName, err)
	}

	dstConn, err := connectors.GetByNameAs[connectors.MirrorDestinationValidationConnector](
		ctx, req.ConnectionConfigs.Env, h.pool, req.ConnectionConfigs.DestinationName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return &protos.ValidateCDCMirrorResponse{}, nil
		}
		return nil, fmt.Errorf("failed to create destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	var tableSchemaMap map[string]*protos.TableSchema
	if !req.ConnectionConfigs.Resync {
		var getTableSchemaError error
		tableSchemaMap, getTableSchemaError = srcConn.GetTableSchema(ctx, req.ConnectionConfigs.Env, req.ConnectionConfigs.Version,
			req.ConnectionConfigs.System, req.ConnectionConfigs.TableMappings)
		if getTableSchemaError != nil {
			return nil, fmt.Errorf("failed to get source table schema: %w", getTableSchemaError)
		}
		for _, tableMapping := range req.ConnectionConfigs.TableMappings {
			srcTableName := tableMapping.SourceTableIdentifier
			if schema, ok := tableSchemaMap[srcTableName]; ok {
				for _, fd := range schema.Columns {
					if fd.Name == tableMapping.PartitionKey && fd.Nullable {
						return nil, fmt.Errorf("partition key cannot not be nullable, nullable column %s detected", tableMapping.PartitionKey)
					}
				}
			}
		}
	}
	if err := dstConn.ValidateMirrorDestination(ctx, req.ConnectionConfigs, tableSchemaMap); err != nil {
		return nil, err
	}

	return &protos.ValidateCDCMirrorResponse{}, nil
}

func (h *FlowRequestHandler) CheckIfMirrorNameExists(ctx context.Context, mirrorName string) (bool, error) {
	var nameExists bool
	if err := h.pool.QueryRow(ctx, "SELECT EXISTS(SELECT * FROM flows WHERE name = $1)", mirrorName).Scan(&nameExists); err != nil {
		return false, fmt.Errorf("failed to check if mirror name exists: %w", err)
	}

	return nameExists, nil
}
