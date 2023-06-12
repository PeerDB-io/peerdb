package model

import (
	"fmt"
)

type QRecord struct {
	NumEntries int
	Entries    []QValue
}

// create a new QRecord with n values
func NewQRecord(n int) *QRecord {
	return &QRecord{
		NumEntries: n,
		Entries:    make([]QValue, n),
	}
}

// Sets the value at the given index
func (q *QRecord) Set(idx int, value QValue) {
	q.Entries[idx] = value
}

// equals checks if two QRecords are identical.
func (q *QRecord) equals(other *QRecord) bool {
	// First check simple attributes
	if q.NumEntries != other.NumEntries {
		return false
	}

	for i, entry := range q.Entries {
		otherEntry := other.Entries[i]
		if !entry.Equals(&otherEntry) {
			fmt.Printf("entry %d: %v != %v\n", i, entry, otherEntry)
			return false
		}
	}

	return true
}

func (q *QRecord) ToAvroCompatibleMap(
	targetDB QDBType,
	nullableFields *map[string]bool,
	colNames []string,
) (map[string]interface{}, error) {
	m := map[string]interface{}{}

	for idx, qValue := range q.Entries {
		key := colNames[idx]
		nullable, ok := (*nullableFields)[key]
		avroVal, err := qValue.ToAvroValue(targetDB, nullable && ok)
		if err != nil {
			return nil, fmt.Errorf("failed to convert QValue to Avro-compatible value: %v", err)
		}
		m[key] = avroVal
	}

	return m, nil
}
