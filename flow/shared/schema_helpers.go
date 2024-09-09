package shared

import (
	"log/slog"
	"maps"
	"slices"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func AdditionalTablesHasOverlap(currentTableMappings []*protos.TableMapping,
	additionalTableMappings []*protos.TableMapping,
) bool {
	currentSrcTables := make([]string, 0, len(currentTableMappings))
	currentDstTables := make([]string, 0, len(currentTableMappings))
	additionalSrcTables := make([]string, 0, len(additionalTableMappings))
	additionalDstTables := make([]string, 0, len(additionalTableMappings))

	for _, currentTableMapping := range currentTableMappings {
		currentSrcTables = append(currentSrcTables, currentTableMapping.SourceTableIdentifier)
		currentDstTables = append(currentDstTables, currentTableMapping.DestinationTableIdentifier)
	}
	for _, additionalTableMapping := range additionalTableMappings {
		additionalSrcTables = append(additionalSrcTables, additionalTableMapping.SourceTableIdentifier)
		additionalDstTables = append(additionalDstTables, additionalTableMapping.DestinationTableIdentifier)
	}

	return ArraysHaveOverlap(currentSrcTables, additionalSrcTables) ||
		ArraysHaveOverlap(currentDstTables, additionalDstTables)
}

// given the output of GetTableSchema, processes it to be used by CDCFlow
// 1) changes the map key to be the destination table name instead of the source table name
// 2) performs column exclusion using protos.TableMapping as input.
func BuildProcessedSchemaMapping(tableMappings []*protos.TableMapping,
	tableNameSchemaMapping map[string]*protos.TableSchema,
	logger log.Logger,
) map[string]*protos.TableSchema {
	sortedSourceTables := slices.Sorted(maps.Keys(tableNameSchemaMapping))
	processedSchemaMapping := make(map[string]*protos.TableSchema, len(sortedSourceTables))

	for _, srcTableName := range sortedSourceTables {
		tableSchema := tableNameSchemaMapping[srcTableName]
		var dstTableName string
		for _, mapping := range tableMappings {
			if mapping.SourceTableIdentifier == srcTableName {
				dstTableName = mapping.DestinationTableIdentifier
				if len(mapping.Exclude) != 0 {
					columnCount := len(tableSchema.Columns)
					columns := make([]*protos.FieldDescription, 0, columnCount)
					for _, column := range tableSchema.Columns {
						if !slices.Contains(mapping.Exclude, column.Name) {
							columns = append(columns, column)
						}
					}
					tableSchema = &protos.TableSchema{
						TableIdentifier:       tableSchema.TableIdentifier,
						PrimaryKeyColumns:     tableSchema.PrimaryKeyColumns,
						IsReplicaIdentityFull: tableSchema.IsReplicaIdentityFull,
						NullableEnabled:       tableSchema.NullableEnabled,
						System:                tableSchema.System,
						Columns:               columns,
					}
				}
				break
			}
		}
		processedSchemaMapping[dstTableName] = tableSchema

		logger.Info("normalized table schema",
			slog.String("table", dstTableName),
			slog.Any("schema", tableSchema))
	}
	return processedSchemaMapping
}
