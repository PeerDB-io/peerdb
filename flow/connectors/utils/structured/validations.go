package structured

import (
	"fmt"
	"regexp"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// ColumnTypeRegex matches the column type expressions accepted as a column's `destination_type`.
var ColumnTypeRegex = regexp.MustCompile(`^$|^[a-zA-Z][a-zA-Z0-9(),]*$`)

// SupportedSourcePeer reports whether peer can be the source of structured ingestion mappings.
// This condition must evolve as new connectors support structured ingestion.
func SupportedSourcePeer(peer *protos.Peer) bool {
	return peer.GetMongoConfig() != nil
}

// ValidateColumns validates the columns of a structured ingestion mapping: at least one, and every one
// declaring a valid destination type, as the mapping carries the destination schema itself and an
// untyped column has nothing to fall back on.
func ValidateColumns(tableIdentifier string, columns []*protos.ColumnSetting) error {
	if len(columns) == 0 {
		return fmt.Errorf("structured ingestion is enabled but no columns are specified for table %s", tableIdentifier)
	}
	columnsWithoutTypes := make([]string, 0, len(columns))
	for _, col := range columns {
		if !ColumnTypeRegex.MatchString(col.DestinationType) {
			return fmt.Errorf("invalid custom column type %s", col.DestinationType)
		}
		if col.DestinationType == "" {
			columnsWithoutTypes = append(columnsWithoutTypes, col.SourceName)
		}
	}
	if len(columnsWithoutTypes) > 0 {
		return fmt.Errorf("structured ingestion is enabled but the following columns have no destination type specified for table %s: %v",
			tableIdentifier, columnsWithoutTypes)
	}
	return nil
}
