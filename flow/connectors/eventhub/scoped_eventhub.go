package conneventhub

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// Scoped eventhub is of the form namespace.eventhub_name.partition_column.partition_key_value
// partition_column is the column in the table that is used to determine
// the partition key for the eventhub. Partition value is one such value of that column.
type ScopedEventhub struct {
	NamespaceName      string
	Eventhub           string
	PartitionKeyColumn string
	PartitionKeyValue  string
}

// The destination QualifiedTable packs "eventhub.partition_key_column" into Table
// (EventHub names containing dots remain unsupported, as before QualifiedTable).
func NewScopedEventhub(destination common.QualifiedTable) (ScopedEventhub, error) {
	eventhubPart, partitionPart, hasDot := strings.Cut(destination.Table, ".")
	if destination.Namespace == "" || !hasDot {
		return ScopedEventhub{}, fmt.Errorf("invalid scoped eventhub '%s'", destination.LegacyDotted())
	}

	// support eventhub name and partition key with hyphens etc.
	return ScopedEventhub{
		NamespaceName:      destination.Namespace,
		Eventhub:           strings.Trim(eventhubPart, `"`),
		PartitionKeyColumn: strings.Trim(partitionPart, `"`),
	}, nil
}

// NewScopedEventhubFromString parses the legacy namespace.eventhub.partition_key_column
// form still used by Lua script destinations.
func NewScopedEventhubFromString(destination string) (ScopedEventhub, error) {
	return NewScopedEventhub(common.NormalizeTableIdentifier(destination))
}

func (s ScopedEventhub) Equals(other ScopedEventhub) bool {
	return s.NamespaceName == other.NamespaceName &&
		s.Eventhub == other.Eventhub &&
		s.PartitionKeyColumn == other.PartitionKeyColumn &&
		s.PartitionKeyValue == other.PartitionKeyValue
}

// ToString returns the string representation of the ScopedEventhub
func (s ScopedEventhub) ToString() string {
	return fmt.Sprintf("%s.%s.%s.%s", s.NamespaceName, s.Eventhub, s.PartitionKeyColumn, s.PartitionKeyValue)
}
