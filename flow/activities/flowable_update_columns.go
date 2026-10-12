package activities

import (
	"context"
	"fmt"
	"slices"

	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/structured"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// UpdateTableColumns adds the columns that are new in the desired column list of each update to the
// destination table and the catalog schema, and returns table mappings with the updated
// TableMapping.Columns/Exclude. Safe to retry: every step is idempotent.
func (a *FlowableActivity) UpdateTableColumns(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	currentMappings []*protos.TableMapping,
	updates []*protos.TableColumnsUpdate,
) ([]*protos.TableMapping, error) {
	logger := internal.LoggerFromCtx(ctx)
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)

	updated := make([]*protos.TableMapping, len(currentMappings))
	for i, tm := range currentMappings {
		updated[i] = proto.CloneOf(tm)
	}

	var toFetch []*protos.TableMapping
	addedByTable := make(map[string][]*protos.ColumnSetting, len(updates))
	for _, update := range updates {
		idx := slices.IndexFunc(updated, func(tm *protos.TableMapping) bool {
			return tm.SourceTableIdentifier == update.SourceTableIdentifier
		})
		if idx == -1 {
			return nil, a.Alerter.LogFlowError(ctx, cfg.FlowJobName,
				fmt.Errorf("table %s is not part of the mirror", update.SourceTableIdentifier))
		}
		tm := updated[idx]

		catalogSchema, err := internal.LoadTableSchemaFromCatalog(ctx, a.CatalogPool, cfg.FlowJobName, tm.DestinationTableIdentifier)
		if err != nil {
			return nil, a.Alerter.LogFlowError(ctx, cfg.FlowJobName,
				fmt.Errorf("failed to load schema of table %s from catalog: %w", tm.DestinationTableIdentifier, err))
		}
		catalogColumns := make([]string, 0, len(catalogSchema.Columns))
		for _, col := range catalogSchema.Columns {
			catalogColumns = append(catalogColumns, col.Name)
		}

		added, err := internal.DiffTableColumns(catalogColumns, tm.Columns, update.Columns)
		if err != nil {
			return nil, a.Alerter.LogFlowError(ctx, cfg.FlowJobName,
				fmt.Errorf("invalid column list for table %s: %w", update.SourceTableIdentifier, err))
		}
		if len(added) == 0 {
			continue
		}

		// columns are applied to the mapping before fetching the source schema so that
		// previously excluded columns show up in it
		tm.Columns = update.Columns
		structured.NormalizeStructuredIngestionTypes(tm.StructuredIngestionConfig, tm.Columns)
		tm.Exclude = slices.DeleteFunc(tm.Exclude, func(name string) bool {
			return slices.ContainsFunc(added, func(cs *protos.ColumnSetting) bool { return cs.SourceName == name })
		})
		addedByTable[tm.SourceTableIdentifier] = added
		toFetch = append(toFetch, tm)
	}
	if len(toFetch) == 0 {
		return updated, nil
	}

	deltas, err := a.buildColumnDeltas(ctx, cfg, toFetch, addedByTable)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}

	peer, dstConn, dstClose, err := connectors.LoadPeerAndGetByNameAs[connectors.CDCSyncConnectorCore](
		ctx, cfg.Env, a.CatalogPool, cfg.DestinationName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, cfg.FlowJobName, fmt.Errorf("failed to get destination connector: %w", err))
	}
	defer dstClose(ctx)
	if !internal.DestinationSupportsColumnUpdates(peer.Type) {
		return nil, a.Alerter.LogFlowError(ctx, cfg.FlowJobName,
			fmt.Errorf("updating columns is not supported for destination type %s", peer.Type))
	}

	// destination first, catalog second: the pull/normalize path reads the catalog schema,
	// so it must never reference a column the destination table lacks
	if err := dstConn.ReplayTableSchemaDeltas(ctx, cfg.Env, cfg.FlowJobName, updated, deltas, cfg.Flags); err != nil {
		return nil, a.Alerter.LogFlowError(ctx, cfg.FlowJobName, fmt.Errorf("failed to add columns to destination: %w", err))
	}
	if err := a.applySchemaDeltas(ctx, cfg, deltas); err != nil {
		return nil, a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}

	logColumnUpdates(logger, deltas)
	return updated, nil
}

func (a *FlowableActivity) buildColumnDeltas(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	tableMappings []*protos.TableMapping,
	addedByTable map[string][]*protos.ColumnSetting,
) ([]*protos.TableSchemaDelta, error) {
	srcConn, srcClose, err := connectors.GetByNameAs[connectors.GetTableSchemaConnector](ctx, cfg.Env, a.CatalogPool, cfg.SourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connector: %w", err)
	}
	defer srcClose(ctx)

	schemas, err := srcConn.GetTableSchema(ctx, cfg.Env, cfg.Version, cfg.System, tableMappings)
	if err != nil {
		return nil, fmt.Errorf("failed to get source table schema: %w", err)
	}

	deltas := make([]*protos.TableSchemaDelta, 0, len(tableMappings))
	for _, tm := range tableMappings {
		schema, ok := schemas[tm.SourceTableIdentifier]
		if !ok {
			return nil, fmt.Errorf("source schema of table %s not found", tm.SourceTableIdentifier)
		}
		fields := make(map[string]*protos.FieldDescription, len(schema.Columns))
		for _, col := range schema.Columns {
			fields[col.Name] = col
		}

		added := addedByTable[tm.SourceTableIdentifier]
		addedFields := make([]*protos.FieldDescription, 0, len(added))
		for _, setting := range added {
			field, ok := fields[setting.SourceName]
			if !ok {
				return nil, fmt.Errorf("column %q not found in source table %s", setting.SourceName, tm.SourceTableIdentifier)
			}
			addedFields = append(addedFields, field)
		}
		deltas = append(deltas, &protos.TableSchemaDelta{
			SrcTableName:    tm.SourceTableIdentifier,
			DstTableName:    tm.DestinationTableIdentifier,
			AddedColumns:    addedFields,
			NullableEnabled: schema.NullableEnabled,
		})
	}
	return deltas, nil
}

func logColumnUpdates(logger log.Logger, deltas []*protos.TableSchemaDelta) {
	for _, delta := range deltas {
		names := make([]string, 0, len(delta.AddedColumns))
		for _, col := range delta.AddedColumns {
			names = append(names, col.Name)
		}
		logger.Info("added columns to mirror table", "table", delta.SrcTableName, "columns", names)
	}
}
