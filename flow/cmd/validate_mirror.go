package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/proto_conversions"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var CustomColumnTypeRegex = regexp.MustCompile(`^$|^[a-zA-Z][a-zA-Z0-9(),]*$`)

type flagConstraint struct {
	ErrorMessage  string
	AffectedTypes []types.QValueKind
}

var FlagConstraints = map[string]flagConstraint{
	shared.Flag_ClickHouseTime64Enabled: {
		AffectedTypes: []types.QValueKind{types.QValueKindTime, types.QValueKindTimeTZ},
		ErrorMessage: "mirror uses time/timetz columns that require ClickHouse setting 'enable_time_time64_type';" +
			" re-enable it or recreate the mirror",
	},
}

// validateQRepIdentifiers checks that a normalized QRepConfig has a usable
// destination (and watermark table when a watermark column is configured).
func validateQRepIdentifiers(cfg *protos.QRepConfig) error {
	if cfg.DestinationTable.GetTable() == "" {
		return errors.New("destination table name is empty")
	}
	if cfg.WatermarkColumn != "" && cfg.QualifiedWatermarkTable.GetTable() == "" {
		return errors.New("watermark table is required when a watermark column is configured")
	}
	return nil
}

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, APIError) {
	if req.ConnectionConfigs != nil {
		// legacy clients (nexus, older UIs) send only dotted string identifiers
		internal.NormalizeFlowConfigAPI(req.ConnectionConfigs)
		if internalVersion, err := internal.PeerDBForceInternalVersion(ctx, req.ConnectionConfigs.Env); err != nil {
			return nil, NewInternalApiError(err)
		} else {
			req.ConnectionConfigs.Version = internalVersion
		}
	}
	flowConnectionConfigsCore := proto_conversions.FlowConnectionConfigsToCore(req.ConnectionConfigs, 0)
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
				fmt.Errorf("mirror with name %s already exists", connectionConfigs.FlowJobName),
				NewMirrorErrorInfo(map[string]string{
					common.ErrorMetadataOffendingField: "flow_job_name",
				}))
		}
	}

	if connectionConfigs.Resync {
		if apiErr := h.checkFlagsCompatibility(ctx, connectionConfigs); apiErr != nil {
			return nil, apiErr
		}
	}

	if connectionConfigs == nil {
		slog.ErrorContext(ctx, "/validatecdc connection configs is nil")
		return nil, NewInvalidArgumentApiError(fmt.Errorf("connection configs is nil"))
	}

	if !connectionConfigs.DoInitialSnapshot && connectionConfigs.InitialSnapshotOnly {
		return nil, NewInvalidArgumentApiError(
			fmt.Errorf("invalid config: initial_snapshot_only is true but do_initial_snapshot is false"))
	}

	if err := validateTableMappingIdentifiers(connectionConfigs.TableMappings); err != nil {
		return nil, NewInvalidArgumentApiError(err)
	}

	for _, tm := range connectionConfigs.TableMappings {
		for _, col := range tm.Columns {
			if !CustomColumnTypeRegex.MatchString(col.DestinationType) {
				return nil, NewInvalidArgumentApiError(fmt.Errorf("invalid custom column type %s", col.DestinationType))
			}
		}
	}

	srcConn, srcClose, err := connectors.GetByNameAs[connectors.MirrorSourceValidationConnector](
		ctx, connectionConfigs.Env, h.pool, connectionConfigs.SourceName,
	)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, NewUnimplementedApiError(fmt.Errorf("connector is not a supported source type"))
		}
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to create source connector: %s", err))
	}
	defer srcClose(ctx)

	if err := srcConn.ValidateMirrorSource(ctx, connectionConfigs); err != nil {
		if missing, ok := errors.AsType[*common.SourceTablesMissingError](err); ok {
			return nil, NewFailedPreconditionApiError(
				missing,
				NewSourceTableMissingErrorInfo(),
				NewSourceTableMissingPreconditionFailure(missing.Tables))
		}
		if notInPub, ok := errors.AsType[*common.TablesNotInPublicationError](err); ok {
			return nil, NewFailedPreconditionApiError(
				notInPub,
				NewTablesNotInPublicationErrorInfo(notInPub.Publication),
				NewTablesNotInPublicationPreconditionFailure(notInPub.Publication, notInPub.Tables))
		}
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

	var tableSchemaMap map[common.QualifiedTable]*protos.TableSchema
	if !connectionConfigs.Resync {
		var getTableSchemaError error
		tableSchemaMap, getTableSchemaError = srcConn.GetTableSchema(ctx, connectionConfigs.Env, connectionConfigs.Version,
			connectionConfigs.System, connectionConfigs.TableMappings)
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

// validateTableMappingIdentifiers rejects duplicate sources/destinations, empty table
// components, and source/destination pairs whose LegacyDotted renderings collide (e.g.
// {"a","b.c"} vs {"a.b","c"}) — colliding destinations would merge in the raw table,
// which stores the dotted format, and colliding sources would produce duplicate
// snapshot clone child-workflow IDs.
func validateTableMappingIdentifiers(tableMappings []*protos.TableMapping) error {
	sources := make(map[common.QualifiedTable]struct{}, len(tableMappings))
	sourcesDotted := make(map[string]common.QualifiedTable, len(tableMappings))
	destinations := make(map[common.QualifiedTable]struct{}, len(tableMappings))
	destinationsDotted := make(map[string]common.QualifiedTable, len(tableMappings))
	for _, tm := range tableMappings {
		source := internal.QualifiedTableFromProto(tm.SourceTable)
		destination := internal.QualifiedTableFromProto(tm.DestinationTable)
		if source.Table == "" {
			return fmt.Errorf("source table name is empty for destination %s", destination)
		}
		if source.Namespace == "" {
			return fmt.Errorf("source namespace is empty for table %s", source.Table)
		}
		if destination.Table == "" {
			return fmt.Errorf("destination table name is empty for source %s", source)
		}
		if _, ok := sources[source]; ok {
			return fmt.Errorf("duplicate source table %s", source)
		}
		sources[source] = struct{}{}
		if other, ok := sourcesDotted[source.LegacyDotted()]; ok {
			return fmt.Errorf("source tables %s and %s are ambiguous with each other due to dots in names",
				source, other)
		}
		sourcesDotted[source.LegacyDotted()] = source
		if _, ok := destinations[destination]; ok {
			return fmt.Errorf("duplicate destination table %s", destination)
		}
		destinations[destination] = struct{}{}
		if other, ok := destinationsDotted[destination.LegacyDotted()]; ok {
			return fmt.Errorf("destination tables %s and %s are ambiguous with each other due to dots in names",
				destination, other)
		}
		destinationsDotted[destination.LegacyDotted()] = destination
	}
	return nil
}

func (h *FlowRequestHandler) checkIfMirrorNameExists(ctx context.Context, mirrorName string) (bool, error) {
	var nameExists bool
	if err := h.pool.QueryRow(ctx, "SELECT EXISTS(SELECT * FROM flows WHERE name = $1)", mirrorName).Scan(&nameExists); err != nil {
		return false, fmt.Errorf("failed to check if mirror name exists: %w", err)
	}

	return nameExists, nil
}

// checkFlagsCompatibility blocks resync when a destination feature flag that was
// enabled at mirror creation is now disabled and the stored schema contains
// column types whose type mapping depends on that flag.
func (h *FlowRequestHandler) checkFlagsCompatibility(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
) APIError {
	newFlags, err := h.determineFlags(ctx, cfg.Env, cfg.DestinationName)
	if err != nil {
		return NewInternalApiError(fmt.Errorf("failed to determine destination flags: %w", err))
	}

	schemaHasColumnTypes := func(colTypes []types.QValueKind) (bool, error) {
		tableNames := make([]common.QualifiedTable, 0, len(cfg.TableMappings))
		for _, tm := range cfg.TableMappings {
			tableNames = append(tableNames, internal.QualifiedTableFromProto(tm.DestinationTable))
		}
		schemas, err := internal.LoadTableSchemasFromCatalog(ctx, h.pool, cfg.FlowJobName, tableNames)
		if err != nil {
			return false, err
		}
		for _, schema := range schemas {
			for _, col := range schema.Columns {
				if slices.Contains(colTypes, types.QValueKind(col.Type)) {
					return true, nil
				}
			}
		}
		return false, nil
	}

	for _, flag := range cfg.Flags {
		if slices.Contains(newFlags, flag) {
			continue
		}
		constraint, ok := FlagConstraints[flag]
		if !ok {
			continue
		}
		affected, err := schemaHasColumnTypes(constraint.AffectedTypes)
		if err != nil {
			return NewInternalApiError(fmt.Errorf("failed to check schema for flag %q: %w", flag, err))
		}
		if affected {
			return NewFailedPreconditionApiError(errors.New(constraint.ErrorMessage))
		}
	}
	return nil
}
