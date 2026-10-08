package cmd

import (
	"context"
	"fmt"
	"slices"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/structured"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// validateColumnsUpdate rejects column list updates that the workflow would fail on, so the
// caller gets the error synchronously instead of the mirror failing after the signal.
// Existence of the columns in the source table is verified by the workflow.
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
		if _, err := internal.DiffTableColumns(catalogColumns, tm.Columns, update.Columns); err != nil {
			return NewInvalidArgumentApiError(fmt.Errorf("invalid column list for table %s: %w", update.SourceTableIdentifier, err))
		}
	}
	return nil
}
