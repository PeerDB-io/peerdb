package connclickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestQValueKindForType(t *testing.T) {
	tests := []struct {
		columnType string
		expected   types.QValueKind
	}{
		// String and its wrappers
		{"String", types.QValueKindString},
		{"Nullable(String)", types.QValueKindString},
		{"LowCardinality(String)", types.QValueKindString},
		{"LowCardinality(Nullable(String))", types.QValueKindString},

		// Bool
		{"Bool", types.QValueKindBoolean},
		{"Nullable(Bool)", types.QValueKindBoolean},

		// Signed integers
		{"Int8", types.QValueKindInt8},
		{"Nullable(Int8)", types.QValueKindInt8},
		{"Int16", types.QValueKindInt16},
		{"Nullable(Int16)", types.QValueKindInt16},
		{"Int32", types.QValueKindInt32},
		{"Nullable(Int32)", types.QValueKindInt32},
		{"Int64", types.QValueKindInt64},
		{"Nullable(Int64)", types.QValueKindInt64},
		{"Int256", types.QValueKindInt256},
		{"Nullable(Int256)", types.QValueKindInt256},

		// Unsigned integers
		{"UInt8", types.QValueKindUInt8},
		{"Nullable(UInt8)", types.QValueKindUInt8},
		{"UInt16", types.QValueKindUInt16},
		{"Nullable(UInt16)", types.QValueKindUInt16},
		{"UInt32", types.QValueKindUInt32},
		{"Nullable(UInt32)", types.QValueKindUInt32},
		{"UInt64", types.QValueKindUInt64},
		{"Nullable(UInt64)", types.QValueKindUInt64},
		{"UInt256", types.QValueKindUInt256},
		{"Nullable(UInt256)", types.QValueKindUInt256},

		// UUID
		{"UUID", types.QValueKindUUID},
		{"Nullable(UUID)", types.QValueKindUUID},

		// Temporal
		{"DateTime64(6)", types.QValueKindTimestamp},
		{"Nullable(DateTime64(6))", types.QValueKindTimestamp},
		{"DateTime64(9)", types.QValueKindTimestamp},
		{"Nullable(DateTime64(9))", types.QValueKindTimestamp},
		{"Time64(6)", types.QValueKindTime},
		{"Nullable(Time64(6))", types.QValueKindTime},
		{"Date32", types.QValueKindDate},
		{"Nullable(Date32)", types.QValueKindDate},

		// Floats
		{"Float32", types.QValueKindFloat32},
		{"Nullable(Float32)", types.QValueKindFloat32},
		{"Float64", types.QValueKindFloat64},
		{"Nullable(Float64)", types.QValueKindFloat64},

		// Arrays
		{"Array(Int32)", types.QValueKindArrayInt32},
		{"Array(Float32)", types.QValueKindArrayFloat32},
		{"Array(Float64)", types.QValueKindArrayFloat64},
		{"Array(String)", types.QValueKindArrayString},
		{"Array(LowCardinality(String))", types.QValueKindArrayString},
		{"Array(UUID)", types.QValueKindArrayUUID},
		{"Array(DateTime64(6))", types.QValueKindArrayTimestamp},
		{"Array(Int64)", types.QValueKindArrayInt64},
		{"Array(Bool)", types.QValueKindArrayBoolean},
		{"Array(Date)", types.QValueKindArrayDate},

		// JSON
		{"JSON", types.QValueKindJSON},
		{"Nullable(JSON)", types.QValueKindJSON},

		// Decimal is matched by substring, with any precision/scale
		{"Decimal(38, 9)", types.QValueKindNumeric},
		{"Decimal(76, 38)", types.QValueKindNumeric},
		{"Decimal128(10)", types.QValueKindNumeric},
		{"Nullable(Decimal(38, 9))", types.QValueKindNumeric},
		{"Array(Decimal(38, 9))", types.QValueKindArrayNumeric},
		{"Array(Nullable(Decimal(38, 9)))", types.QValueKindArrayNumeric},
	}

	for _, tt := range tests {
		t.Run(tt.columnType, func(t *testing.T) {
			kind, err := QValueKindForType(tt.columnType)
			require.NoError(t, err)
			require.Equal(t, tt.expected, kind)
		})
	}
}

func TestQValueKindForTypeUnsupported(t *testing.T) {
	for _, columnType := range []string{
		"",
		"NotAType",
		"Int128",
		"Date",
		"DateTime",
		"DateTime64(3)",
		"Nullable(Array(Int32))",
		"Array(Int8)",
		"Map(String, String)",
		"Tuple(Int32, String)",
		"FixedString(16)",
		"Enum8('a' = 1)",
		"IPv4",
		"IPv6",
	} {
		t.Run(columnType, func(t *testing.T) {
			kind, err := QValueKindForType(columnType)
			require.ErrorContains(t, err, "failed to resolve QValueKind for "+columnType)
			require.Equal(t, types.QValueKindInvalid, kind)
		})
	}
}
