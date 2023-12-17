package model

import (
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type QRecord struct {
	NumEntries int
	Entries    []qvalue.QValue
}

// create a new QRecord with n values
func NewQRecord(n int) *QRecord {
	return &QRecord{
		NumEntries: n,
		Entries:    make([]qvalue.QValue, n),
	}
}

// Sets the value at the given index
func (q *QRecord) Set(idx int, value qvalue.QValue) {
	q.Entries[idx] = value
}
