package conneventhub

import (
	"fmt"
	"strings"
)

// Scoped eventhub is of the form peer_name.eventhub_name.partition_column.partition_key_value
// partition_column is the column in the table that is used to determine
// the partition key for the eventhub. Partition value is one such value of that column.
type ScopedEventhub struct {
	PeerName           string
	Eventhub           string
	PartitionKeyColumn string
	PartitionKeyValue  string
}

func NewScopedEventhub(dstTableName string) (ScopedEventhub, error) {
	// split by dot, the model is peername.eventhub.partition_key_column.partition_key_value
	parts := strings.Split(dstTableName, ".")

	if len(parts) != 3 {
		return ScopedEventhub{}, fmt.Errorf("invalid scoped eventhub '%s'", dstTableName)
	}

	// support eventhub name and partition key with hyphens etc.
	eventhubPart := strings.Trim(parts[1], `"`)
	partitionPart := strings.Trim(parts[2], `"`)
	return ScopedEventhub{
		PeerName:           parts[0],
		Eventhub:           eventhubPart,
		PartitionKeyColumn: partitionPart,
	}, nil
}

func (s *ScopedEventhub) SetPartitionValue(value string) {
	s.PartitionKeyValue = value
}

func (s ScopedEventhub) Equals(other ScopedEventhub) bool {
	return s.PeerName == other.PeerName &&
		s.Eventhub == other.Eventhub &&
		s.PartitionKeyColumn == other.PartitionKeyColumn &&
		s.PartitionKeyValue == other.PartitionKeyValue
}

// ToString returns the string representation of the ScopedEventhub
func (s ScopedEventhub) ToString() string {
	return fmt.Sprintf("%s.%s.%s.%s", s.PeerName, s.Eventhub, s.PartitionKeyColumn, s.PartitionKeyValue)
}
