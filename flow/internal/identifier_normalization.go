package internal

import (
	"cmp"
	"slices"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// This file is the boundary between the legacy dotted-string table identifiers and
// QualifiedTable structs. Inputs arriving from the API, from configs persisted before
// the struct migration, or from Temporal payloads recorded by older releases carry only
// the legacy strings; the Normalize* functions populate the struct fields from them
// (first-dot-split semantics) and clear the strings so internal code only ever sees
// structs. All functions are pure, total and idempotent, which makes them safe to call
// at Temporal workflow entry (deterministic on replay). The Denormalize* functions fill
// the legacy strings back in for outbound API responses and, for one release, for
// persisted configs (rollback safety).

func normalizeQualifiedTable(qt *protos.QualifiedTable, legacy *string) *protos.QualifiedTable {
	if qt == nil && *legacy != "" {
		qt = QualifiedTableProto(common.NormalizeTableIdentifier(*legacy))
	}
	*legacy = ""
	return qt
}

func denormalizeQualifiedTable(qt *protos.QualifiedTable, legacy *string) {
	if qt != nil {
		*legacy = QualifiedTableFromProto(qt).LegacyDotted()
	}
}

func sortQualifiedTableMappings(mappings []*protos.QualifiedTableMapping) {
	slices.SortFunc(mappings, func(a, b *protos.QualifiedTableMapping) int {
		return cmp.Or(
			cmp.Compare(a.Source.GetNamespace(), b.Source.GetNamespace()),
			cmp.Compare(a.Source.GetTable(), b.Source.GetTable()),
		)
	})
}

func NormalizeTableMapping(tm *protos.TableMapping) {
	tm.SourceTable = normalizeQualifiedTable(tm.SourceTable, &tm.SourceTableIdentifier)
	tm.DestinationTable = normalizeQualifiedTable(tm.DestinationTable, &tm.DestinationTableIdentifier)
}

func NormalizeTableMappings(tableMappings []*protos.TableMapping) {
	for _, tm := range tableMappings {
		NormalizeTableMapping(tm)
	}
}

func DenormalizeTableMappings(tableMappings []*protos.TableMapping) {
	for _, tm := range tableMappings {
		denormalizeQualifiedTable(tm.SourceTable, &tm.SourceTableIdentifier)
		denormalizeQualifiedTable(tm.DestinationTable, &tm.DestinationTableIdentifier)
	}
}

func NormalizeFlowConfig(cfg *protos.FlowConnectionConfigsCore) {
	if cfg != nil {
		NormalizeTableMappings(cfg.TableMappings)
	}
}

//nolint:gocritic // boundary helper: normalizes the external API message before conversion to Core
func NormalizeFlowConfigAPI(cfg *protos.FlowConnectionConfigs) {
	if cfg != nil {
		NormalizeTableMappings(cfg.TableMappings)
	}
}

// DenormalizeFlowConfigForAPI fills legacy string fields (keeping the structs) so that
// API consumers reading only the pre-struct fields, like nexus and older UIs, keep
// working. Also applied to configs persisted in the catalog for one release, so a
// rollback to the previous release can still read them.
//
//nolint:gocritic // boundary helper: refills legacy fields on the external API message
func DenormalizeFlowConfigForAPI(cfg *protos.FlowConnectionConfigs) {
	if cfg != nil {
		DenormalizeTableMappings(cfg.TableMappings)
	}
}

func DenormalizeFlowConfigCore(cfg *protos.FlowConnectionConfigsCore) {
	if cfg != nil {
		DenormalizeTableMappings(cfg.TableMappings)
	}
}

func NormalizeQRepConfig(cfg *protos.QRepConfig) {
	if cfg == nil {
		return
	}
	cfg.QualifiedWatermarkTable = normalizeQualifiedTable(cfg.QualifiedWatermarkTable, &cfg.WatermarkTable)
	cfg.DestinationTable = normalizeQualifiedTable(cfg.DestinationTable, &cfg.DestinationTableIdentifier)
}

func DenormalizeQRepConfig(cfg *protos.QRepConfig) {
	if cfg == nil {
		return
	}
	denormalizeQualifiedTable(cfg.QualifiedWatermarkTable, &cfg.WatermarkTable)
	denormalizeQualifiedTable(cfg.DestinationTable, &cfg.DestinationTableIdentifier)
}

func DenormalizeSyncFlowOptions(opts *protos.SyncFlowOptions) {
	if opts == nil {
		return
	}
	DenormalizeTableMappings(opts.TableMappings)
	if len(opts.SrcTableIdNameMapping) == 0 && len(opts.SrcTableIdMapping) > 0 {
		opts.SrcTableIdNameMapping = make(map[uint32]string, len(opts.SrcTableIdMapping))
		for relID, table := range opts.SrcTableIdMapping {
			opts.SrcTableIdNameMapping[relID] = QualifiedTableFromProto(table).LegacyDotted()
		}
	}
}

func DenormalizeDropFlowInput(input *protos.DropFlowInput) {
	if input != nil {
		DenormalizeFlowConfigCore(input.FlowConnectionConfigs)
	}
}

func NormalizeSyncFlowOptions(opts *protos.SyncFlowOptions) {
	if opts == nil {
		return
	}
	NormalizeTableMappings(opts.TableMappings)
	if opts.SrcTableIdMapping == nil && len(opts.SrcTableIdNameMapping) > 0 {
		opts.SrcTableIdMapping = make(map[uint32]*protos.QualifiedTable, len(opts.SrcTableIdNameMapping))
		for relID, name := range opts.SrcTableIdNameMapping {
			opts.SrcTableIdMapping[relID] = QualifiedTableProto(common.NormalizeTableIdentifier(name))
		}
	}
	opts.SrcTableIdNameMapping = nil
}

func NormalizeCDCFlowConfigUpdate(update *protos.CDCFlowConfigUpdate) {
	if update == nil {
		return
	}
	NormalizeTableMappings(update.AdditionalTables)
	NormalizeTableMappings(update.RemovedTables)
}

func NormalizeDropFlowInput(input *protos.DropFlowInput) {
	if input != nil {
		NormalizeFlowConfig(input.FlowConnectionConfigs)
	}
}

func NormalizeTableSchemaDelta(delta *protos.TableSchemaDelta) {
	if delta == nil {
		return
	}
	delta.SrcTable = normalizeQualifiedTable(delta.SrcTable, &delta.SrcTableName)
	delta.DstTable = normalizeQualifiedTable(delta.DstTable, &delta.DstTableName)
}

func NormalizeTableSchemaDeltas(deltas []*protos.TableSchemaDelta) {
	for _, delta := range deltas {
		NormalizeTableSchemaDelta(delta)
	}
}

func NormalizeTableSchema(ts *protos.TableSchema) {
	if ts != nil {
		ts.Table = normalizeQualifiedTable(ts.Table, &ts.TableIdentifier)
	}
}

func DenormalizeTableSchema(ts *protos.TableSchema) {
	if ts != nil {
		denormalizeQualifiedTable(ts.Table, &ts.TableIdentifier)
	}
}

func NormalizeEnsurePullabilityInput(input *protos.EnsurePullabilityBatchInput) {
	if input == nil {
		return
	}
	if input.SourceTables == nil && len(input.SourceTableIdentifiers) > 0 {
		input.SourceTables = make([]*protos.QualifiedTable, 0, len(input.SourceTableIdentifiers))
		for _, identifier := range input.SourceTableIdentifiers {
			input.SourceTables = append(input.SourceTables, QualifiedTableProto(common.NormalizeTableIdentifier(identifier)))
		}
	}
	input.SourceTableIdentifiers = nil
}

func normalizeStringTableMap(legacy map[string]string, mappings []*protos.QualifiedTableMapping) []*protos.QualifiedTableMapping {
	if mappings == nil && len(legacy) > 0 {
		mappings = make([]*protos.QualifiedTableMapping, 0, len(legacy))
		for source, destination := range legacy {
			mappings = append(mappings, &protos.QualifiedTableMapping{
				Source:      QualifiedTableProto(common.NormalizeTableIdentifier(source)),
				Destination: QualifiedTableProto(common.NormalizeTableIdentifier(destination)),
			})
		}
		sortQualifiedTableMappings(mappings)
	}
	return mappings
}

func NormalizeSetupReplicationInput(input *protos.SetupReplicationInput) {
	if input == nil {
		return
	}
	input.QualifiedTableMappings = normalizeStringTableMap(input.TableNameMapping, input.QualifiedTableMappings)
	input.TableNameMapping = nil
}

func NormalizeCreateRawTableInput(input *protos.CreateRawTableInput) {
	if input == nil {
		return
	}
	input.QualifiedTableMappings = normalizeStringTableMap(input.TableNameMapping, input.QualifiedTableMappings)
	input.TableNameMapping = nil
}

func NormalizeCreateTablesFromExistingInput(input *protos.CreateTablesFromExistingInput) {
	if input == nil {
		return
	}
	input.NewToExisting = normalizeStringTableMap(input.NewToExistingTableMapping, input.NewToExisting)
	input.NewToExistingTableMapping = nil
}

func NormalizeRenameTablesInput(input *protos.RenameTablesInput) {
	if input == nil {
		return
	}
	for _, option := range input.RenameTableOptions {
		option.CurrentTable = normalizeQualifiedTable(option.CurrentTable, &option.CurrentName)
		option.NewTable = normalizeQualifiedTable(option.NewTable, &option.NewName)
	}
}

func NormalizeRemoveTablesFromRawTableInput(input *protos.RemoveTablesFromRawTableInput) {
	if input == nil {
		return
	}
	if input.DestinationTables == nil && len(input.DestinationTableNames) > 0 {
		input.DestinationTables = make([]*protos.QualifiedTable, 0, len(input.DestinationTableNames))
		for _, name := range input.DestinationTableNames {
			input.DestinationTables = append(input.DestinationTables, QualifiedTableProto(common.NormalizeTableIdentifier(name)))
		}
	}
	input.DestinationTableNames = nil
}

func NormalizeQRepPartition(partition *protos.QRepPartition) {
	if partition == nil {
		return
	}
	for _, childRange := range partition.ChildTableRanges {
		childRange.ChildTable = normalizeQualifiedTable(childRange.ChildTable, &childRange.Table)
	}
}

func NormalizeSetupTableSchemaBatchInput(input *protos.SetupTableSchemaBatchInput) {
	if input != nil {
		NormalizeTableMappings(input.TableMappings)
	}
}

func NormalizeSetupNormalizedTableBatchInput(input *protos.SetupNormalizedTableBatchInput) {
	if input != nil {
		NormalizeTableMappings(input.TableMappings)
	}
}

func NormalizeAddTablesToPublicationInput(input *protos.AddTablesToPublicationInput) {
	if input != nil {
		NormalizeTableMappings(input.AdditionalTables)
	}
}

func NormalizeRemoveTablesFromPublicationInput(input *protos.RemoveTablesFromPublicationInput) {
	if input != nil {
		NormalizeTableMappings(input.TablesToRemove)
	}
}
