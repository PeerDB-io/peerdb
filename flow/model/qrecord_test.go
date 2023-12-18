package model

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Equals checks if two QRecordBatches are identical.
func (q *QRecordBatch) Equals(other *QRecordBatch) bool {
	if other == nil {
		fmt.Printf("other is nil")
		return q == nil
	}

	// First check simple attributes
	if q.NumRecords != other.NumRecords {
		// print num records
		fmt.Printf("q.NumRecords: %d\n", q.NumRecords)
		fmt.Printf("other.NumRecords: %d\n", other.NumRecords)
		return false
	}

	// Compare column names
	if !q.Schema.EqualNames(other.Schema) {
		fmt.Printf("Column names are not equal\n")
		fmt.Printf("Schema 1: %v\n", q.Schema.GetColumnNames())
		fmt.Printf("Schema 2: %v\n", other.Schema.GetColumnNames())
		return false
	}

	// Compare records
	for i, record := range q.Records {
		if !record.equals(other.Records[i]) {
			fmt.Printf("Record %d is not equal\n", i)
			fmt.Printf("Record 1: %v\n", record)
			fmt.Printf("Record 2: %v\n", other.Records[i])
			return false
		}
	}

	return true
}

// equals checks if two QRecords are identical.
func (q *QRecord) equals(other *QRecord) bool {
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

func TestEquals(t *testing.T) {
	uuidVal1, _ := uuid.NewRandom()
	uuidVal2, _ := uuid.NewRandom()

	tests := []struct {
		name string
		q1   QRecord
		q2   QRecord
		want bool
	}{
		{
			name: "Equal - Same UUID",
			q1: QRecord{
				NumEntries: 1,
				Entries:    []qvalue.QValue{{Kind: qvalue.QValueKindUUID, Value: uuidVal1}},
			},
			q2: QRecord{
				NumEntries: 1,
				Entries: []qvalue.QValue{
					{Kind: qvalue.QValueKindString, Value: uuidVal1.String()},
				},
			},
			want: true,
		},
		{
			name: "Not Equal - Different UUID",
			q1: QRecord{
				NumEntries: 1,
				Entries:    []qvalue.QValue{{Kind: qvalue.QValueKindUUID, Value: uuidVal1}},
			},
			q2: QRecord{
				NumEntries: 1,
				Entries:    []qvalue.QValue{{Kind: qvalue.QValueKindUUID, Value: uuidVal2}},
			},
			want: false,
		},
		{
			name: "Equal - Same numeric",
			q1: QRecord{
				NumEntries: 1,
				Entries: []qvalue.QValue{
					{Kind: qvalue.QValueKindNumeric, Value: big.NewRat(10, 2)},
				},
			},
			q2: QRecord{
				NumEntries: 1,
				Entries:    []qvalue.QValue{{Kind: qvalue.QValueKindString, Value: "5"}},
			},
			want: true,
		},
		{
			name: "Not Equal - Different numeric",
			q1: QRecord{
				NumEntries: 1,
				Entries: []qvalue.QValue{
					{Kind: qvalue.QValueKindNumeric, Value: big.NewRat(10, 2)},
				},
			},
			q2: QRecord{
				NumEntries: 1,
				Entries:    []qvalue.QValue{{Kind: qvalue.QValueKindNumeric, Value: "4.99"}},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.q1.equals(tt.q2), tt.want)
		})
	}
}
