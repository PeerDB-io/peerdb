package cmd

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/structured"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// validateColumnsUpdate rejects column list updates that the workflow would fail on, so the
// caller gets the error synchronously instead of the mirror failing after the signal.
// The workflow re-checks everything, in case the source or catalog changes before it runs.
func (h *FlowRequestHandler) validateColumnsUpdate(
	ctx context.Context,
	flowJobName string,
	updates []*protos.TableColumnsUpdate,
) APIError {
	cfg, err := internal.FetchConfigFromDB(ctx, h.pool, flowJobName)
	if err != nil {
		return NewInternalApiError(err)
	}

	destPeer, err := connectors.LoadPeer(ctx, h.pool, cfg.DestinationName)
	if err != nil {
		return NewInternalApiError(fmt.Errorf("unable to load destination peer: %w", err))
	}
	if !internal.DestinationSupportsColumnUpdates(destPeer.Type) {
		return NewInvalidArgumentApiError(fmt.Errorf("columns update is not supported for destination type %s", destPeer.Type))
	}

	seenTables := make(map[string]struct{}, len(updates))
	addedByTable := make(map[string][]*protos.ColumnSetting, len(updates))
	var mappingsToCheck []*protos.TableMapping
	for _, update := range updates {
		if _, dup := seenTables[update.SourceTableIdentifier]; dup {
			return NewInvalidArgumentApiError(fmt.Errorf("table %s listed more than once", update.SourceTableIdentifier))
		}
		seenTables[update.SourceTableIdentifier] = struct{}{}

		idx := slices.IndexFunc(cfg.TableMappings, func(tm *protos.TableMapping) bool {
			return tm.SourceTableIdentifier == update.SourceTableIdentifier
		})
		if idx == -1 {
			return NewInvalidArgumentApiError(fmt.Errorf("table %s is not part of the mirror", update.SourceTableIdentifier))
		}
		tm := cfg.TableMappings[idx]

		for _, col := range update.Columns {
			if !structured.ColumnTypeRegex.MatchString(col.DestinationType) {
				return NewInvalidArgumentApiError(fmt.Errorf("invalid destination_type %q for column %q", col.DestinationType, col.SourceName))
			}
		}

		catalogSchema, err := internal.LoadTableSchemaFromCatalog(ctx, h.pool, flowJobName, tm.DestinationTableIdentifier)
		if err != nil {
			return NewInternalApiError(fmt.Errorf("unable to load schema of table %s: %w", tm.DestinationTableIdentifier, err))
		}
		catalogColumns := make([]string, 0, len(catalogSchema.Columns))
		for _, col := range catalogSchema.Columns {
			catalogColumns = append(catalogColumns, col.Name)
		}
		added, err := internal.DiffTableColumns(catalogColumns, tm.Columns, update.Columns)
		if err != nil {
			return NewInvalidArgumentApiError(fmt.Errorf("invalid column list for table %s: %w", update.SourceTableIdentifier, err))
		}
		if len(added) == 0 {
			continue
		}

		// previously excluded columns must show up in the source schema
		checked := proto.CloneOf(tm)
		checked.Exclude = slices.DeleteFunc(checked.Exclude, func(name string) bool {
			return slices.ContainsFunc(added, func(cs *protos.ColumnSetting) bool { return cs.SourceName == name })
		})
		mappingsToCheck = append(mappingsToCheck, checked)
		addedByTable[tm.SourceTableIdentifier] = added
	}

	return h.checkColumnsExistInSource(ctx, cfg, mappingsToCheck, addedByTable)
}

// checkColumnsExistInSource verifies that every added column exists in its source table.
func (h *FlowRequestHandler) checkColumnsExistInSource(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	tableMappings []*protos.TableMapping,
	addedByTable map[string][]*protos.ColumnSetting,
) APIError {
	if len(tableMappings) == 0 {
		return nil
	}

	srcConn, srcClose, err := connectors.GetByNameAs[connectors.GetTableSchemaConnector](ctx, cfg.Env, h.pool, cfg.SourceName)
	if err != nil {
		return NewInternalApiError(fmt.Errorf("failed to connect to source peer %s: %w", cfg.SourceName, err))
	}
	defer srcClose(ctx)

	schemas, err := srcConn.GetTableSchema(ctx, cfg.Env, cfg.Version, cfg.System, tableMappings)
	if err != nil {
		return NewInternalApiError(fmt.Errorf("failed to get source table schema: %w", err))
	}

	for _, tm := range tableMappings {
		schema, ok := schemas[tm.SourceTableIdentifier]
		if !ok {
			return NewInternalApiError(fmt.Errorf("source schema of table %s not found", tm.SourceTableIdentifier))
		}
		for _, setting := range addedByTable[tm.SourceTableIdentifier] {
			if !slices.ContainsFunc(schema.Columns, func(col *protos.FieldDescription) bool { return col.Name == setting.SourceName }) {
				return NewInvalidArgumentApiError(
					fmt.Errorf("column %q not found in source table %s", setting.SourceName, tm.SourceTableIdentifier))
			}
		}
	}
	return nil
}
