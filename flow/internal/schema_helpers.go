package internal

import (
	"cmp"
	"log/slog"
	"slices"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func AdditionalTablesHasOverlap(currentTableMappings []*protos.TableMapping,
	additionalTableMappings []*protos.TableMapping,
	checkDestination bool,
) bool {
	currentSrcTables := make(map[common.QualifiedTable]struct{}, len(currentTableMappings))
	var currentDstTables map[common.QualifiedTable]struct{}
	if checkDestination {
		currentDstTables = make(map[common.QualifiedTable]struct{}, len(currentTableMappings))
	}
	for _, currentTableMapping := range currentTableMappings {
		currentSrcTables[QualifiedTableFromProto(currentTableMapping.SourceTable)] = struct{}{}
		if checkDestination {
			currentDstTables[QualifiedTableFromProto(currentTableMapping.DestinationTable)] = struct{}{}
		}
	}
	for _, additionalTableMapping := range additionalTableMappings {
		if _, exists := currentSrcTables[QualifiedTableFromProto(additionalTableMapping.SourceTable)]; exists {
			return true
		}
		if checkDestination {
			if _, exists := currentDstTables[QualifiedTableFromProto(additionalTableMapping.DestinationTable)]; exists {
				return true
			}
		}
	}
	return false
}

func CompareQualifiedTables(a common.QualifiedTable, b common.QualifiedTable) int {
	return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Table, b.Table))
}

func SortedQualifiedTables(tables map[common.QualifiedTable]*protos.TableSchema) []common.QualifiedTable {
	sorted := make([]common.QualifiedTable, 0, len(tables))
	for table := range tables {
		sorted = append(sorted, table)
	}
	slices.SortFunc(sorted, CompareQualifiedTables)
	return sorted
}

// given the output of GetTableSchema, processes it to be used by CDCFlow
// 1) changes the map key to be the destination table name instead of the source table name
// 2) performs column exclusion using protos.TableMapping as input.
func BuildProcessedSchemaMapping(
	tableMappings []*protos.TableMapping,
	tableNameSchemaMapping map[common.QualifiedTable]*protos.TableSchema,
	logger log.Logger,
) map[common.QualifiedTable]*protos.TableSchema {
	sortedSourceTables := SortedQualifiedTables(tableNameSchemaMapping)
	processedSchemaMapping := make(map[common.QualifiedTable]*protos.TableSchema, len(sortedSourceTables))

	for _, srcTableName := range sortedSourceTables {
		tableSchema := tableNameSchemaMapping[srcTableName]
		var dstTableName common.QualifiedTable
		for _, mapping := range tableMappings {
			if QualifiedTableFromProto(mapping.SourceTable) == srcTableName {
				dstTableName = QualifiedTableFromProto(mapping.DestinationTable)
				if len(mapping.Exclude) != 0 {
					columns := make([]*protos.FieldDescription, 0, len(tableSchema.Columns))
					pkeyColumns := make([]string, 0, len(tableSchema.PrimaryKeyColumns))
					for _, column := range tableSchema.Columns {
						if !slices.Contains(mapping.Exclude, column.Name) {
							columns = append(columns, column)
						}
						if slices.Contains(tableSchema.PrimaryKeyColumns, column.Name) &&
							!slices.Contains(mapping.Exclude, column.Name) {
							pkeyColumns = append(pkeyColumns, column.Name)
						}
					}
					tableSchema = &protos.TableSchema{
						Table:                 tableSchema.Table,
						PrimaryKeyColumns:     pkeyColumns,
						IsReplicaIdentityFull: tableSchema.IsReplicaIdentityFull,
						NullableEnabled:       tableSchema.NullableEnabled,
						System:                tableSchema.System,
						Columns:               columns,
						TableOid:              tableSchema.TableOid,
					}
				}
				break
			}
		}
		processedSchemaMapping[dstTableName] = tableSchema

		logger.Info("normalized table schema",
			slog.String("table", dstTableName.String()),
			slog.Any("schema", tableSchema))
	}
	return processedSchemaMapping
}
