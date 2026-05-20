package internal

import (
	"log/slog"
	"maps"
	"slices"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func AdditionalTablesHasOverlap(currentTableMappings []*protos.TableMapping,
	additionalTableMappings []*protos.TableMapping,
	checkDestination bool,
) bool {
	currentSrcTables := make(map[string]struct{}, len(currentTableMappings))
	var currentDstTables map[string]struct{}
	if checkDestination {
		currentDstTables = make(map[string]struct{}, len(currentTableMappings))
	}
	for _, currentTableMapping := range currentTableMappings {
		currentSrcTables[currentTableMapping.SourceTableIdentifier] = struct{}{}
		if checkDestination {
			currentDstTables[currentTableMapping.DestinationTableIdentifier] = struct{}{}
		}
	}
	for _, additionalTableMapping := range additionalTableMappings {
		if _, exists := currentSrcTables[additionalTableMapping.SourceTableIdentifier]; exists {
			return true
		}
		if checkDestination {
			if _, exists := currentDstTables[additionalTableMapping.DestinationTableIdentifier]; exists {
				return true
			}
		}
	}
	return false
}

// given the output of GetTableSchema, processes it to be used by CDCFlow
// 1) changes the map key to be the destination table name instead of the source table name
// 2) performs column exclusion using protos.TableMapping as input.
func BuildProcessedSchemaMapping(
	tableMappings []*protos.TableMapping,
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
						TableIdentifier:       tableSchema.TableIdentifier,
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
			slog.String("table", dstTableName),
			slog.Any("schema", tableSchema))
	}
	return processedSchemaMapping
}
