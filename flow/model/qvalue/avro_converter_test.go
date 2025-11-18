package qvalue

import (
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestColumnNameAvroFieldConvert(t *testing.T) {
	testColumnNames := []string{
		"valid_column",             // Already valid
		"ColumnWithCaps",           // Mixed case
		"column-name",              // Hyphen should be replaced with _
		"column.name",              // Dot should be replaced with _
		"column@name",              // Special character should be replaced with _
		"!invalid_start",           // Invalid start character
		"123numericStart",          // Starts with a number, should be prefixed with _
		"UPPER_CASE_COLUMN",        // Already valid
		"column name",              // Space should be replaced with _
		"column$name",              // Special character should be replaced with _
		"column#name",              // Special character should be replaced with _
		"column&name",              // Special character should be replaced with _
		"123",                      // Fully numeric, should be prefixed with _
		"_already_valid",           // Already valid
		"column__name",             // Already valid with underscores
		"table.column",             // Dotted name (common in queries), should be replaced with _
		"column-name-with-hyphens", // Multiple hyphens
		" spaces  in  name ",       // Multiple spaces should be replaced
		"trailing_",                // Should remain unchanged
		"__leading_underscores",    // Already valid, should remain
		"CAPS-WITH-HYPHEN",         // Hyphen should be replaced with _
		"",                         // Empty input, should return "_"
	}

	expectedColumnNames := []string{
		"valid_column",
		"ColumnWithCaps",
		"column_name",
		"column_name",
		"column_name",
		"_invalid_start",
		"_123numericStart",
		"UPPER_CASE_COLUMN",
		"column_name",
		"column_name",
		"column_name",
		"column_name",
		"_123",
		"_already_valid",
		"column__name",
		"table_column",
		"column_name_with_hyphens",
		"_spaces__in__name_",
		"trailing_",
		"__leading_underscores",
		"CAPS_WITH_HYPHEN",
		"_",
	}

	for i, columnName := range testColumnNames {
		t.Run(columnName, func(t *testing.T) {
			assert.Equal(t, expectedColumnNames[i], ConvertToAvroCompatibleName(columnName))
		})
	}
}

func TestAvroQValueSize(t *testing.T) {
	tests := []struct {
		qvalue       types.QValue
		name         string
		expectedSize int64
	}{
		// Primitive fixed-size types
		{
			name:         "float32",
			qvalue:       types.QValueFloat32{Val: 3.14},
			expectedSize: 4,
		},
		{
			name:         "float64",
			qvalue:       types.QValueFloat64{Val: 3.14159},
			expectedSize: 8,
		},
		{
			name:         "boolean",
			qvalue:       types.QValueBoolean{Val: true},
			expectedSize: 1,
		},
		{
			name:         "timestamp",
			qvalue:       types.QValueTimestamp{Val: time.Now()},
			expectedSize: 8,
		},
		{
			name:         "timestamptz",
			qvalue:       types.QValueTimestampTZ{Val: time.Now()},
			expectedSize: 8,
		},
		{
			name:         "date",
			qvalue:       types.QValueDate{Val: time.Now()},
			expectedSize: 4,
		},
		{
			name:         "time",
			qvalue:       types.QValueTime{Val: time.Since(time.Unix(0, 0))},
			expectedSize: 8,
		},
		{
			name:         "timetz",
			qvalue:       types.QValueTimeTZ{Val: time.Since(time.Unix(0, 0))},
			expectedSize: 8,
		},
		{
			name:         "uuid",
			qvalue:       types.QValueUUID{Val: uuid.New()},
			expectedSize: 37, // UUID as string: varint(36) + 36 chars
		},

		// Int256/UInt256 (fixed 32-byte arrays)
		{
			name:         "int256",
			qvalue:       types.QValueInt256{Val: big.NewInt(12345)},
			expectedSize: 32, // fixed 32-byte array
		},
		{
			name:         "uint256",
			qvalue:       types.QValueUInt256{Val: big.NewInt(67890)},
			expectedSize: 32, // fixed 32-byte array
		},

		// Variable-length integer types
		{
			name:         "int8 small value",
			qvalue:       types.QValueInt8{Val: 5},
			expectedSize: 1,
		},
		{
			name:         "int16 small value",
			qvalue:       types.QValueInt16{Val: 100},
			expectedSize: 2,
		},
		{
			name:         "int32 small value",
			qvalue:       types.QValueInt32{Val: 127},
			expectedSize: 2,
		},
		{
			name:         "int64 large value",
			qvalue:       types.QValueInt64{Val: 1000000000},
			expectedSize: 5,
		},
		{
			name:         "uint8",
			qvalue:       types.QValueUInt8{Val: 200},
			expectedSize: 2,
		},
		{
			name:         "uint16",
			qvalue:       types.QValueUInt16{Val: 5000},
			expectedSize: 2,
		},
		{
			name:         "uint32",
			qvalue:       types.QValueUInt32{Val: 100000},
			expectedSize: 3,
		},
		{
			name:         "uint64",
			qvalue:       types.QValueUInt64{Val: 1000000000},
			expectedSize: 5,
		},

		// String types (varint length + bytes)
		{
			name:         "string empty",
			qvalue:       types.QValueString{Val: ""},
			expectedSize: 1, // varint(0)
		},
		{
			name:         "string short",
			qvalue:       types.QValueString{Val: "hello"},
			expectedSize: 6, // varint(5) + 5 bytes
		},
		{
			name:         "qchar single byte",
			qvalue:       types.QValueQChar{Val: 'A'},
			expectedSize: 2, // varint(1) + 1 byte
		},

		// Numeric (decimal as string)
		{
			name:         "numeric small",
			qvalue:       types.QValueNumeric{Val: decimal.NewFromFloat(123.45)},
			expectedSize: 7, // "123.45" = varint(6) + 6 chars
		},

		// Bytes type
		{
			name:         "bytes empty",
			qvalue:       types.QValueBytes{Val: []byte{}},
			expectedSize: 1, // varint(0)
		},
		{
			name:         "bytes small",
			qvalue:       types.QValueBytes{Val: []byte{1, 2, 3, 4, 5}},
			expectedSize: 6, // varint(5) + 5 bytes
		},

		// JSON (as string)
		{
			name:         "json",
			qvalue:       types.QValueJSON{Val: `{"key":"value"}`},
			expectedSize: 16, // 15 chars + varint(15)
		},

		// Arrays
		{
			name:         "array int32 empty",
			qvalue:       types.QValueArrayInt32{Val: []int32{}},
			expectedSize: 1, // varint(0) for empty array block
		},
		{
			name:         "array string small",
			qvalue:       types.QValueArrayString{Val: []string{"a", "b"}},
			expectedSize: 6, // varint(2) + (varint(1)+1) + (varint(1)+1) + varint(0)
		},
	}

	for _, tt := range tests {
		for _, nullable := range []bool{false, true} {
			t.Run(tt.name, func(t *testing.T) {
				size := ComputeAvroSize(tt.qvalue, nullable, nil)
				if nullable {
					assert.Equal(t, tt.expectedSize+1, size, "size mismatch for %s", tt.name)
				} else {
					assert.Equal(t, tt.expectedSize, size, "size mismatch for %s", tt.name)
				}
			})

		}
	}
}

func TestAvroQValueSizeNilValue(t *testing.T) {
	qval := types.QValueInt256{Val: nil}
	size := ComputeAvroSize(qval, true, nil)
	assert.Equal(t, int64(1), size, "nil value should return just union tag size")
}

func TestVarIntSize(t *testing.T) {
	tests := []struct {
		value        int64
		expectedSize int64
	}{
		{0, 1},
		{1, 1},
		{63, 1},
		{64, 2},
		{127, 2},
		{128, 2},
		{8191, 2},
		{8192, 3},
		{-1, 1},
		{-64, 1},
		{-65, 2},
		{1000000000, 5},
		{-1000000000, 5},
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.value)), func(t *testing.T) {
			size := varIntSize(tt.value)
			assert.Equal(t, tt.expectedSize, size, "varIntSize(%d) should be %d", tt.value, tt.expectedSize)
		})
	}
}

func TestStringSize(t *testing.T) {
	tests := []struct {
		str          string
		expectedSize int64
	}{
		{"", 1},
		{"a", 2},
		{"hello", 6},
		{"hello world", 12},
		{string(make([]byte, 127)), 129},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			size := stringSize(tt.str)
			require.Equal(t, tt.expectedSize, size, "stringSize should be %d", tt.expectedSize)
		})
	}
}
