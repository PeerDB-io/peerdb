package structured

import (
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	chvalidate "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
)

// SupportedSourcePeer reports whether peer can be the source of structured ingestion mappings
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
		if col.DestinationType == "" {
			columnsWithoutTypes = append(columnsWithoutTypes, col.SourceName)
		} else if err := chvalidate.ValidateColumnType(col.DestinationType); err != nil {
			return fmt.Errorf("table %s: column %s: %w", tableIdentifier, col.SourceName, err)
		}
	}
	if len(columnsWithoutTypes) > 0 {
		return fmt.Errorf("structured ingestion is enabled but the following columns have no destination type specified for table %s: %v",
			tableIdentifier, columnsWithoutTypes)
	}
	return nil
}
