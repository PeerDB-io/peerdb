package model

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type QRecord struct {
	NumEntries int
	Entries    []qvalue.QValue
}

// create a new QRecord with n values
func NewQRecord(n int) QRecord {
	return QRecord{
		NumEntries: n,
		Entries:    make([]qvalue.QValue, n),
	}
}

// Sets the value at the given index
func (q *QRecord) Set(idx int, value qvalue.QValue) {
	q.Entries[idx] = value
}

// equals checks if two QRecords are identical.
func (q QRecord) equals(other QRecord) bool {
	// First check simple attributes
	if q.NumEntries != other.NumEntries {
		return false
	}

	for i, entry := range q.Entries {
		otherEntry := other.Entries[i]
		if !entry.Equals(otherEntry) {
			fmt.Printf("entry %d: %v != %v\n", i, entry, otherEntry)
			return false
		}
	}

	return true
}
