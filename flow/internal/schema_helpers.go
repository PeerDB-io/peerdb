package internal

import (
	"errors"
	"fmt"
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

// ValidateResyncTables checks that each table to resync is an existing mirror table with the same
// destination, is requested once, and is not also added or removed in the same update.
// Other fields of the resync mappings are the new table settings.
func ValidateResyncTables(currentTableMappings []*protos.TableMapping, update *protos.CDCFlowConfigUpdate) error {
	currentBySource := make(map[string]*protos.TableMapping, len(currentTableMappings))
	for _, mapping := range currentTableMappings {
		currentBySource[mapping.SourceTableIdentifier] = mapping
	}

	resyncSources := make(map[string]struct{}, len(update.ResyncTables))
	for _, requested := range update.ResyncTables {
		if requested == nil || requested.SourceTableIdentifier == "" {
			return errors.New("resync table must have a source table identifier")
		}
		if _, duplicate := resyncSources[requested.SourceTableIdentifier]; duplicate {
			return fmt.Errorf("table %s was requested for resync more than once", requested.SourceTableIdentifier)
		}
		resyncSources[requested.SourceTableIdentifier] = struct{}{}

		current, exists := currentBySource[requested.SourceTableIdentifier]
		if !exists {
			return fmt.Errorf("table %s is not part of the mirror", requested.SourceTableIdentifier)
		}
		if requested.DestinationTableIdentifier != current.DestinationTableIdentifier {
			return fmt.Errorf("destination for table %s does not match the mirror configuration",
				requested.SourceTableIdentifier)
		}
	}

	for _, table := range update.AdditionalTables {
		if _, exists := resyncSources[table.SourceTableIdentifier]; exists {
			return fmt.Errorf("table %s cannot be added and resynced in the same update",
				table.SourceTableIdentifier)
		}
	}
	for _, table := range update.RemovedTables {
		if _, exists := resyncSources[table.SourceTableIdentifier]; exists {
			return fmt.Errorf("table %s cannot be removed and resynced in the same update",
				table.SourceTableIdentifier)
		}
	}
	return nil
}
