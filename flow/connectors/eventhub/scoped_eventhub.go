package conneventhub

import (
	"fmt"
	"strings"
)

// Scoped eventhub is of the form peer_name.eventhub_name.partition_column.partition_key_value
// partition_column is the column in the table that is used to determine
// the partition key for the eventhub. Partition value is one such value of that column.
type ScopedEventhub struct {
	EventhubNamespace  string
	Eventhub           string
	PartitionKeyColumn string
	PartitionKeyValue  string
}

func NewScopedEventhub(dstTableName string) (ScopedEventhub, error) {
	// split by dot, the model is eventhub_namespace.eventhub_name.table_name.partition_key_column
	parts := strings.Split(dstTableName, ".")

	if len(parts) != 4 {
		return ScopedEventhub{}, fmt.Errorf("invalid scoped eventhub '%s'", dstTableName)
	}

	// support eventhub namespace, eventhub name, partition key with hyphens etc.
	// part[2] will be some table identifier.
	// It's just so that we have distinct destination table names
	// in create mirror's table mapping.
	// We can ignore it.
	namespacePart := strings.Trim(parts[0], `"`)
	eventhubPart := strings.Trim(parts[1], `"`)
	partitionPart := strings.Trim(parts[3], `"`)
	return ScopedEventhub{
		EventhubNamespace:  namespacePart,
		Eventhub:           eventhubPart,
		PartitionKeyColumn: partitionPart,
	}, nil
}

func (s *ScopedEventhub) SetPartitionValue(value string) {
	s.PartitionKeyValue = value
}

// ToString returns the string representation of the ScopedEventhub
func (s ScopedEventhub) ToString() string {
	return fmt.Sprintf("%s.%s.%s.%s", s.EventhubNamespace, s.Eventhub, s.PartitionKeyColumn, s.PartitionKeyValue)
}
