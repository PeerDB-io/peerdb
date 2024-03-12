package model_test

import (
	"math/big"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func TestEquals(t *testing.T) {
	uuidVal1, _ := uuid.NewRandom()
	uuidVal2, _ := uuid.NewRandom()

	tests := []struct {
		name string
		q1   []qvalue.QValue
		q2   []qvalue.QValue
		want bool
	}{
		{
			name: "Equal - Same UUID",
			q1:   []qvalue.QValue{{Kind: qvalue.QValueKindUUID, Value: uuidVal1}},
			q2:   []qvalue.QValue{{Kind: qvalue.QValueKindString, Value: uuidVal1.String()}},
			want: true,
		},
		{
			name: "Not Equal - Different UUID",
			q1:   []qvalue.QValue{{Kind: qvalue.QValueKindUUID, Value: uuidVal1}},
			q2:   []qvalue.QValue{{Kind: qvalue.QValueKindUUID, Value: uuidVal2}},
			want: false,
		},
		{
			name: "Equal - Same numeric",
			q1:   []qvalue.QValue{{Kind: qvalue.QValueKindNumeric, Value: big.NewRat(10, 2)}},
			q2:   []qvalue.QValue{{Kind: qvalue.QValueKindString, Value: "5"}},
			want: true,
		},
		{
			name: "Not Equal - Different numeric",
			q1:   []qvalue.QValue{{Kind: qvalue.QValueKindNumeric, Value: big.NewRat(10, 2)}},
			q2:   []qvalue.QValue{{Kind: qvalue.QValueKindNumeric, Value: "4.99"}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, e2eshared.CheckQRecordEquality(t, tt.q1, tt.q2))
		})
	}
}
