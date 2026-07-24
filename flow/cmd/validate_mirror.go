package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"

	"google.golang.org/protobuf/proto"

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

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, APIError) {
	if req.ConnectionConfigs != nil {
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

	for _, tm := range connectionConfigs.TableMappings {
		for _, col := range tm.Columns {
			if !CustomColumnTypeRegex.MatchString(col.DestinationType) {
				return nil, NewInvalidArgumentApiError(fmt.Errorf("invalid custom column type %s", col.DestinationType))
			}
		}
	}

	if apiErr := h.checkSourcePeerReuse(ctx, connectionConfigs); apiErr != nil {
		return nil, apiErr
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
		if replicaIdErr, ok := errors.AsType[*common.ReplicaIdentifierInUseError](err); ok {
			// Beyond other PeerDB mirrors, a replica id must not be already
			// registered at source.
			// We only enforce this check if the mirror doesn't already exist as
			// a resync in that case might happen while the mirror is not paused thus
			// detecting the same mirror as a conflict.
			if mirrorExists, err := h.checkIfMirrorNameExists(ctx, connectionConfigs.FlowJobName); err != nil {
				return nil, NewInternalApiError(
					fmt.Errorf("failed to check if mirror name exists: %w", err))
			} else if !mirrorExists {
				return nil, NewFailedPreconditionApiError(
					fmt.Errorf("source peer %q pins a replica id = %s, which is already in use by a replica registered on the source database",
						connectionConfigs.SourceName, replicaIdErr.Id),
					NewMirrorErrorInfo(map[string]string{
						common.ErrorMetadataOffendingField: "source_name",
					}))
			}
		} else {
			return nil, NewFailedPreconditionApiError(
				fmt.Errorf("failed to validate source connector %s: %w", connectionConfigs.SourceName, err))
		}
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

// checkSourcePeerReuse rejects a CDC mirror whose MySQL source peer pins a fixed server_id while
// that peer already backs another streaming CDC mirror. A fixed server_id can only be used by one
// concurrent binlog connection, so sharing such a peer across mirrors makes their replicas collide
// on the source. Peers without a fixed server_id fall back to a random per-connection id and are
// safe to reuse.
func (h *FlowRequestHandler) checkSourcePeerReuse(
	ctx context.Context, cfg *protos.FlowConnectionConfigsCore,
) APIError {
	// A pinned server_id only matters for CDC; a snapshot-only mirror never opens a binlog connection.
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	peer, err := connectors.LoadPeer(ctx, h.pool, cfg.SourceName)
	if err != nil {
		return NewInternalApiError(fmt.Errorf("failed to load source peer %s: %w", cfg.SourceName, err))
	}
	mysqlCfg := peer.GetMysqlConfig()
	if mysqlCfg == nil || mysqlCfg.ServerId == nil {
		return nil
	}

	// CDC flows for this source peer other than the mirror being validated.
	// query_string IS NULL filters out QRep flows, which do
	// not stream the binlog.
	query := `
	SELECT f.name name, f.config_proto config_proto
	FROM flows f
	JOIN peers p ON f.source_peer = p.id AND p.name = $1
	WHERE f.name != $2 AND f.config_proto IS NOT NULL AND f.query_string IS NULL
	`

	rows, err := h.pool.Query(ctx, query, cfg.SourceName, cfg.FlowJobName)
	if err != nil {
		return NewInternalApiError(fmt.Errorf("failed to check source peer reuse for %s: %w", cfg.SourceName, err))
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var configBytes []byte
		if err := rows.Scan(&name, &configBytes); err != nil {
			return NewInternalApiError(fmt.Errorf("failed to read flow row while checking peer reuse: %w", err))
		}
		var existing protos.FlowConnectionConfigsCore
		if err := proto.Unmarshal(configBytes, &existing); err != nil {
			return NewInternalApiError(fmt.Errorf("failed to unmarshal config for flow %s: %w", name, err))
		}
		// A snapshot-only mirror never streams the binlog, so it cannot collide on server_id.
		if existing.DoInitialSnapshot && existing.InitialSnapshotOnly {
			continue
		}
		return NewFailedPreconditionApiError(
			fmt.Errorf("source peer %q pins server_id=%d, which cannot be shared across mirrors; "+
				"it is already used by CDC mirror %q. Use a distinct source peer (or one without a fixed server_id) for this mirror",
				cfg.SourceName, mysqlCfg.GetServerId(), name),
			NewMirrorErrorInfo(map[string]string{
				common.ErrorMetadataOffendingField: "source_name",
			}))
	}
	if err := rows.Err(); err != nil {
		return NewInternalApiError(fmt.Errorf("failed to iterate flows while checking peer reuse for %s: %w", cfg.SourceName, err))
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
		tableNames := make([]string, 0, len(cfg.TableMappings))
		for _, tm := range cfg.TableMappings {
			tableNames = append(tableNames, tm.DestinationTableIdentifier)
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
