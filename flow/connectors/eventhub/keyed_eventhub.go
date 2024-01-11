package conneventhub

import "fmt"

// KeyedScopedEventhub is of the form peer_name.eventhub_name.partition_column.partition_key
type KeyedScopedEventhub struct {
	ScopedEventhub ScopedEventhub
	PartitionKey   string
}

func NewKeyedScopedEventhub(scopedEventhub ScopedEventhub, partitionKey string) KeyedScopedEventhub {
	return KeyedScopedEventhub{
		ScopedEventhub: scopedEventhub,
		PartitionKey:   partitionKey,
	}
}

func (ke KeyedScopedEventhub) Equals(other KeyedScopedEventhub) bool {
	return ke.ScopedEventhub.Equals(other.ScopedEventhub) &&
		ke.PartitionKey == other.PartitionKey
}

// ToString returns the string representation of the KeyedScopedEventhub
func (ke KeyedScopedEventhub) ToString() string {
	return fmt.Sprintf("%s.%s", ke.ScopedEventhub.ToString(), ke.PartitionKey)
}
