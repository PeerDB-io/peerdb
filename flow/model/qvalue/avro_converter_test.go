package qvalue

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
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

		// Special types (as string)
		{
			name:         "cidr",
			qvalue:       types.QValueCIDR{Val: "192.168.1.0/24"},
			expectedSize: 15, // varint(14) + 14 chars
		},
		{
			name:         "inet",
			qvalue:       types.QValueINET{Val: "192.168.1.1"},
			expectedSize: 12, // varint(11) + 11 chars
		},
		{
			name:         "macaddr",
			qvalue:       types.QValueMacaddr{Val: "08:00:2b:01:02:03"},
			expectedSize: 18, // varint(17) + 17 chars
		},
		{
			name:         "interval",
			qvalue:       types.QValueInterval{Val: "1 day"},
			expectedSize: 6, // varint(5) + 5 chars
		},
		{
			name:         "enum",
			qvalue:       types.QValueEnum{Val: "active"},
			expectedSize: 7, // varint(6) + 6 chars
		},
		{
			name:         "geography",
			qvalue:       types.QValueGeography{Val: "POINT(1 1)"},
			expectedSize: 11, // varint(10) + 10 chars
		},
		{
			name:         "geometry",
			qvalue:       types.QValueGeometry{Val: "POINT(2 2)"},
			expectedSize: 11, // varint(10) + 10 chars
		},
		{
			name:         "point",
			qvalue:       types.QValuePoint{Val: "(1,2)"},
			expectedSize: 6, // varint(5) + 5 chars
		},
		{
			name:         "invalid",
			qvalue:       types.QValueInvalid{Val: "bad_value"},
			expectedSize: 10, // varint(9) + 9 chars
		},

		// Arrays
		{
			name:         "array int16 with values",
			qvalue:       types.QValueArrayInt16{Val: []int16{10, 20, 30}},
			expectedSize: 5, // varint(3) + 1 + 1 + 1 + terminator(1)
		},
		{
			name:         "array int32 with values",
			qvalue:       types.QValueArrayInt32{Val: []int32{1, 2, 3}},
			expectedSize: 5, // varint(3) + 1 + 1 + 1 + terminator(1)
		},
		{
			name:         "array int64 with values",
			qvalue:       types.QValueArrayInt64{Val: []int64{100, 200, 300}},
			expectedSize: 8, // varint(3) + 2 + 2 + 2 + terminator(1)
		},
		{
			name:         "array float32 with values",
			qvalue:       types.QValueArrayFloat32{Val: []float32{1.1, 2.2, 3.3}},
			expectedSize: 14, // varint(3) + 4*3 + terminator(1)
		},
		{
			name:         "array float64 with values",
			qvalue:       types.QValueArrayFloat64{Val: []float64{1.1, 2.2, 3.3}},
			expectedSize: 26, // varint(3) + 8*3 + terminator(1)
		},
		{
			name:         "array boolean with values",
			qvalue:       types.QValueArrayBoolean{Val: []bool{true, false, true}},
			expectedSize: 5, // varint(3) + 1*3 + terminator(1)
		},
		{
			name:         "array string small",
			qvalue:       types.QValueArrayString{Val: []string{"a", "b"}},
			expectedSize: 6, // varint(2) + (varint(1)+1) + (varint(1)+1) + terminator(1)
		},
		{
			name:         "array enum",
			qvalue:       types.QValueArrayEnum{Val: []string{"red", "blue"}},
			expectedSize: 11, // varint(2) + (varint(3)+3) + (varint(4)+4) + terminator(1)
		},
		{
			name:         "array interval",
			qvalue:       types.QValueArrayInterval{Val: []string{"1 day", "2 hours"}},
			expectedSize: 16, // varint(2) + (varint(5)+5) + (varint(7)+7) + terminator(1)
		},
		{
			name:         "array timestamp with values",
			qvalue:       types.QValueArrayTimestamp{Val: []time.Time{time.Now(), time.Now()}},
			expectedSize: 18, // varint(2) + 8*2 + terminator(1)
		},
		{
			name:         "array timestamptz with values",
			qvalue:       types.QValueArrayTimestampTZ{Val: []time.Time{time.Now(), time.Now()}},
			expectedSize: 18, // varint(2) + 8*2 + terminator(1)
		},
		{
			name:         "array date with values",
			qvalue:       types.QValueArrayDate{Val: []time.Time{time.Now(), time.Now()}},
			expectedSize: 10, // varint(2) + 4*2 + terminator(1)
		},
		{
			name:         "array uuid with values",
			qvalue:       types.QValueArrayUUID{Val: []uuid.UUID{uuid.New(), uuid.New()}},
			expectedSize: 76, // varint(2) + (varint(36)+36)*2 + terminator(1)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := &types.QField{Nullable: false}
			_, size, err := QValueToAvro(context.Background(), tt.qvalue, field, protos.DBType_CLICKHOUSE,
				log.NewStructuredLogger(nil), false, nil, internal.BinaryFormatRaw, true)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedSize, size, "size mismatch for %s", tt.name)
		})
		t.Run(tt.name+"_nullable", func(t *testing.T) {
			field := &types.QField{Nullable: true}
			_, size, err := QValueToAvro(context.Background(), tt.qvalue, field, protos.DBType_CLICKHOUSE,
				log.NewStructuredLogger(nil), false, nil, internal.BinaryFormatRaw, true)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedSize+1, size, "size mismatch for %s", tt.name)
		})
		t.Run(tt.name+"_no_calc_size", func(t *testing.T) {
			field := &types.QField{Nullable: false}
			_, size, err := QValueToAvro(context.Background(), tt.qvalue, field, protos.DBType_CLICKHOUSE,
				log.NewStructuredLogger(nil), false, nil, internal.BinaryFormatRaw, false)
			require.NoError(t, err)
			assert.Equal(t, int64(0), size, "size mismatch for %s", tt.name)
		})
	}
}

func TestConstSize(t *testing.T) {
	tests := []struct {
		name         string
		n            int64
		sizeOpt      sizeOpt
		expectedSize int64
	}{
		// n + nullable
		{"simple [123+0]", 123, sizePlain, 123},
		{"simple [0+0]", 0, sizePlain, 0},
		{"with nullable [4+1]", 4, sizeNullable, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := constSize(tt.n, tt.sizeOpt)
			assert.Equal(t, tt.expectedSize, size)
		})
	}
}

func TestVarIntSize(t *testing.T) {
	tests := []struct {
		name         string
		value        int64
		expectedSize int64
	}{
		{"zero", 0, 1},
		{"small positive", 1, 1},
		{"max 1-byte positive", 63, 1},
		{"min 2-byte positive", 64, 2},
		{"max 2-byte positive", 8191, 2},
		{"min 3-byte positive", 8192, 3},
		{"small negative", -1, 1},
		{"max 1-byte negative", -64, 1},
		{"min 2-byte negative", -65, 2},
		{"max 2-byte negative", -8192, 2},
		{"min 3-byte negative", -8193, 3},
		{"large positive", 1000000000, 5},
		{"large negative", -1000000000, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := varIntSize(tt.value, sizePlain)
			assert.Equal(t, tt.expectedSize, size)
		})
	}
}

func TestStringSize(t *testing.T) {
	tests := []struct {
		name         string
		str          string
		sizeOpt      sizeOpt
		expectedSize int64
	}{
		// varint(count) + len(string) + nullable
		{"empty string [1+0+0]", "", sizePlain, 1},
		{"single char [1+1+0]]", "a", sizePlain, 2},
		{"simple string [1+11+0]", "hello world", sizePlain, 12},
		{"longer string [2+127+0]", string(make([]byte, 127)), sizePlain, 129},
		{"with nullable [2+127+1]", string(make([]byte, 127)), sizeNullable, 130},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := stringSize(tt.str, tt.sizeOpt)
			require.Equal(t, tt.expectedSize, size)
		})
	}
}

func TestFixedArraySize(t *testing.T) {
	tests := []struct {
		name         string
		count        int
		elemSize     int64
		sizeOpt      sizeOpt
		expectedSize int64
	}{
		// nullable + varint(count) + (elemSize * count) + terminator if non-empty
		{"empty array [0+1+0+0]", 0, 8, sizePlain, 1},
		{"single element [0+1+8+1]", 1, 8, sizePlain, 10},
		{"multiple elements [0+1+5*8+1]", 5, 8, sizePlain, 42},
		{"with nullable [1+1+3*4+1]", 3, 4, sizeNullable, 15},
		{"large count [0+2+100*1+1]", 100, 1, sizePlain, 103},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := fixedArraySize(tt.count, tt.elemSize, tt.sizeOpt)
			assert.Equal(t, tt.expectedSize, size)
		})
	}
}

func TestStringArraySize(t *testing.T) {
	tests := []struct {
		name         string
		vals         []string
		sizeOpt      sizeOpt
		expectedSize int64
	}{
		// nullable + varint(count) + sum(elem sizes) + terminator(if non-empty)
		{"empty array [0+1+0+0]", []string{}, sizePlain, 1},
		{"single elem [0+1+(1+1)+1]", []string{"a"}, sizePlain, 4},
		{"multiple elems [0+1+((1+0)+(1+2)+(1+3))+1]", []string{"", "ab", "xyz"}, sizePlain, 10},
		{"with nullable [1+1+(1+4)+1]", []string{"test"}, sizeNullable, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := stringArraySize(tt.vals, tt.sizeOpt)
			assert.Equal(t, tt.expectedSize, size)
		})
	}
}

func TestDecimalSize(t *testing.T) {
	tests := []struct {
		name         string
		num          decimal.Decimal
		precision    int16
		scale        int16
		sizeOpt      sizeOpt
		expectedSize int64
	}{
		// name format: "value [varint+int_bytes+frac_bytes(+nullable)?]"
		{"0 [1+1+0]", decimal.NewFromInt(0), 76, 38, sizePlain, 2},
		{"1.23 [1+1+16]", decimal.NewFromFloat(1.23), 76, 38, sizePlain, 18},
		{"-1.23 [1+1+16]", decimal.NewFromFloat(-1.23), 76, 38, sizePlain, 18},
		{"1000 [1+2+16]", decimal.NewFromInt(1000), 76, 38, sizePlain, 19},
		{"9999999999 [1+5+16]", decimal.NewFromInt(9999999999), 76, 38, sizePlain, 22},
		{"123.456789 [1+1+16]", decimal.NewFromFloat(123.456789), 76, 38, sizePlain, 18},
		{"127 [1+1+16]", decimal.NewFromInt(127), 76, 38, sizePlain, 18},
		{"-128 [1+1+16]", decimal.NewFromInt(-128), 76, 38, sizePlain, 18},
		{"128 [1+1+16]", decimal.NewFromInt(128), 76, 38, sizePlain, 18},
		{"-129 [1+1+16]", decimal.NewFromInt(-129), 76, 38, sizePlain, 18},
		{"0.123 [1+0+16]", decimal.NewFromFloat(0.123), 76, 38, sizePlain, 17},
		{"1234567890 [1+5+16]", decimal.NewFromInt(1234567890), 76, 38, sizePlain, 22},
		{"99.99 [1+1+16+1]", decimal.NewFromFloat(99.99), 76, 38, sizeNullable, 19},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := QValueAvroConverter{
				QField: &types.QField{
					Precision: tt.precision,
					Scale:     tt.scale,
				},
				TargetDWH: protos.DBType_CLICKHOUSE,
			}
			_, size := c.processNumeric(tt.num, tt.sizeOpt)
			// computedSize may overestimate by up to 1 byte due to log2(10) estimation rounding
			assert.True(t, size == tt.expectedSize || size == tt.expectedSize+1,
				"size %d should equal expectedSize %d or expectedSize+1", size, tt.expectedSize)
		})
	}

	// unboundedNumericAsString=true encodes decimals as strings
	stringTests := []struct {
		name         string
		num          decimal.Decimal
		expectedSize int64
	}{
		// size = varint(len) + string chars
		{"123.45 [1+6]", decimal.NewFromFloat(123.45), 7},
		{"0 [1+1]", decimal.NewFromInt(0), 2},
		{"-999.999 [1+8]", decimal.NewFromFloat(-999.999), 9},
	}

	for _, tt := range stringTests {
		t.Run("string/"+tt.name, func(t *testing.T) {
			c := QValueAvroConverter{
				QField:                   &types.QField{},
				TargetDWH:                protos.DBType_CLICKHOUSE,
				UnboundedNumericAsString: true,
			}
			_, size := c.processNumeric(tt.num, sizePlain)
			assert.Equal(t, tt.expectedSize, size)
		})
	}
}
