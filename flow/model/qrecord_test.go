package model

import (
	"math/big"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestEquals(t *testing.T) {
	uuidVal1, _ := uuid.NewRandom()
	uuidVal2, _ := uuid.NewRandom()

	tests := []struct {
		name string
		q1   *QRecord
		q2   *QRecord
		want bool
	}{
		{
			name: "Equal - Same UUID",
			q1:   &QRecord{NumEntries: 1, Entries: []QValue{{Kind: QValueKindUUID, Value: uuidVal1}}},
			q2:   &QRecord{NumEntries: 1, Entries: []QValue{{Kind: QValueKindString, Value: uuidVal1.String()}}},
			want: true,
		},
		{
			name: "Not Equal - Different UUID",
			q1:   &QRecord{NumEntries: 1, Entries: []QValue{{Kind: QValueKindUUID, Value: uuidVal1}}},
			q2:   &QRecord{NumEntries: 1, Entries: []QValue{{Kind: QValueKindUUID, Value: uuidVal2}}},
			want: false,
		},
		{
			name: "Equal - Same numeric",
			q1:   &QRecord{NumEntries: 1, Entries: []QValue{{Kind: QValueKindNumeric, Value: big.NewRat(10, 2)}}},
			q2:   &QRecord{NumEntries: 1, Entries: []QValue{{Kind: QValueKindString, Value: "5"}}},
			want: true,
		},
		{
			name: "Not Equal - Different numeric",
			q1:   &QRecord{NumEntries: 1, Entries: []QValue{{Kind: QValueKindNumeric, Value: big.NewRat(10, 2)}}},
			q2:   &QRecord{NumEntries: 1, Entries: []QValue{{Kind: QValueKindNumeric, Value: "4.99"}}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.q1.equals(tt.q2), tt.want)
		})
	}
}
