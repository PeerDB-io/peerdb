package qvalue

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestGetNumericDestinationTypeClickHouse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		precision, scale         int16
		unboundedNumericAsString bool
		want                     NumericDestinationType
	}{
		{"unbounded", 0, 0, false, NumericDestinationType{Precision: 76, Scale: 38}},
		{"unbounded as string", 0, 0, true, NumericDestinationType{IsString: true}},
		{"bounded within limits", 10, 2, false, NumericDestinationType{Precision: 10, Scale: 2}},
		{"precision above 76", 100, 2, false, NumericDestinationType{IsString: true}},
		{"scale above precision", 3, 5, false, NumericDestinationType{IsString: true}},
		// Postgres encodes a negative scale in 11-bit two's complement, which ParseNumericTypmod reads as a large positive.
		{"negative scale as parsed from typmod", 5, 2045, false, NumericDestinationType{IsString: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := GetNumericDestinationType(tc.precision, tc.scale, protos.DBType_CLICKHOUSE, tc.unboundedNumericAsString)
			require.Equal(t, tc.want, got)
		})
	}
}
