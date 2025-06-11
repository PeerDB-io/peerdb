package model_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestEquals(t *testing.T) {
	uuidVal1 := uuid.New()
	uuidVal2 := uuid.New()

	tests := []struct {
		name string
		q1   []types.QValue
		q2   []types.QValue
		want bool
	}{
		{
			name: "Equal - Same UUID",
			q1:   []types.QValue{types.QValueUUID{Val: uuidVal1}},
			q2:   []types.QValue{types.QValueString{Val: uuidVal1.String()}},
			want: true,
		},
		{
			name: "Not Equal - Different UUID",
			q1:   []types.QValue{types.QValueUUID{Val: uuidVal1}},
			q2:   []types.QValue{types.QValueUUID{Val: uuidVal2}},
			want: false,
		},
		{
			name: "Equal - Same numeric",
			q1:   []types.QValue{types.QValueNumeric{Val: decimal.NewFromInt(5)}},
			q2:   []types.QValue{types.QValueString{Val: "5"}},
			want: true,
		},
		{
			name: "Not Equal - Different numeric",
			q1:   []types.QValue{types.QValueNumeric{Val: decimal.NewFromInt(5)}},
			q2:   []types.QValue{types.QValueString{Val: "4.99"}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, e2eshared.CheckQRecordEquality(t, tt.q1, tt.q2))
		})
	}
}
