package shared

import (
	"github.com/PeerDB-io/peer-flow/connectors/utils"
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

	return utils.ArraysHaveOverlap(currentSrcTables, additionalSrcTables) ||
		utils.ArraysHaveOverlap(currentDstTables, additionalDstTables)
}
