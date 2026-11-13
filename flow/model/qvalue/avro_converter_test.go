package qvalue

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
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
	ctx := context.Background()
	env := map[string]string{
		"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "raw",
		"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
	}

	tests := []types.QValue{
		// Primitive fixed-size types
		types.QValueFloat32{Val: 3.14},
		types.QValueFloat64{Val: 3.14159},
		types.QValueBoolean{Val: true},
		types.QValueTimestamp{Val: time.Now()},
		types.QValueTimestampTZ{Val: time.Now()},
		types.QValueDate{Val: time.Now()},
		types.QValueTime{Val: time.Since(time.Unix(0, 0))},
		types.QValueTimeTZ{Val: time.Since(time.Unix(0, 0))},
		types.QValueUUID{Val: uuid.New()},

		// Int256/UInt256 (fixed 32-byte arrays)
		types.QValueInt256{Val: big.NewInt(12345)},
		types.QValueUInt256{Val: big.NewInt(67890)},

		// Variable-length integer types
		types.QValueInt8{Val: 5},
		types.QValueInt16{Val: 100},
		types.QValueInt32{Val: 127},
		types.QValueInt64{Val: 1000000000},
		types.QValueUInt8{Val: 200},
		types.QValueUInt16{Val: 5000},
		types.QValueUInt32{Val: 100000},
		types.QValueUInt64{Val: 1000000000},

		// String types (varint length + bytes)
		types.QValueString{Val: ""},
		types.QValueString{Val: "hello"},
		types.QValueQChar{Val: 'A'},

		// Bytes type
		types.QValueBytes{Val: []byte{}},
		types.QValueBytes{Val: []byte{1, 2, 3, 4, 5}},

		// JSON (as string)
		types.QValueJSON{Val: `{"key":"value"}`},

		// Special types (as string)
		types.QValueCIDR{Val: "192.168.1.0/24"},
		types.QValueINET{Val: "192.168.1.1"},
		types.QValueMacaddr{Val: "08:00:2b:01:02:03"},
		types.QValueInterval{Val: "1 day"},
		types.QValueEnum{Val: "active"},
		types.QValueGeography{Val: "POINT(1 1)"},
		types.QValueGeometry{Val: "POINT(2 2)"},
		types.QValuePoint{Val: "(1,2)"},
		types.QValueInvalid{Val: "bad_value"},

		// Numeric (value doesn't cause overcount)
		types.QValueNumeric{Val: decimal.NewFromInt(1000), Precision: 10, Scale: 0},

		// Arrays
		types.QValueArrayInt16{Val: []int16{10, 20, 30}},
		types.QValueArrayInt32{Val: []int32{1, 2, 3}},
		types.QValueArrayInt64{Val: []int64{100, 200, 300}},
		types.QValueArrayFloat32{Val: []float32{1.1, 2.2, 3.3}},
		types.QValueArrayFloat64{Val: []float64{1.1, 2.2, 3.3}},
		types.QValueArrayBoolean{Val: []bool{true, false, true}},
		types.QValueArrayString{Val: []string{"a", "b"}},
		types.QValueArrayEnum{Val: []string{"red", "blue"}},
		types.QValueArrayInterval{Val: []string{"1 day", "2 hours"}},
		types.QValueArrayTimestamp{Val: []time.Time{time.Now(), time.Now()}},
		types.QValueArrayTimestampTZ{Val: []time.Time{time.Now(), time.Now()}},
		types.QValueArrayDate{Val: []time.Time{time.Now(), time.Now()}},
		types.QValueArrayUUID{Val: []uuid.UUID{uuid.New(), uuid.New()}},
		// Values don't cause overcount
		types.QValueArrayNumeric{Val: []decimal.Decimal{decimal.NewFromInt(1000), decimal.NewFromInt(2000)}, Precision: 10, Scale: 0},
	}

	for _, qv := range tests {
		field := types.QField{Type: qv.Kind(), Nullable: false}
		// Extract precision/scale for numeric types
		switch v := qv.(type) {
		case types.QValueNumeric:
			field.Precision, field.Scale = v.Precision, v.Scale
		case types.QValueArrayNumeric:
			field.Precision, field.Scale = v.Precision, v.Scale
		}

		t.Run(string(qv.Kind()), func(t *testing.T) {
			schema := getAvroSchema(t, ctx, env, qv, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, qv, &field, false)
			actualSize := avroEncodedSize(t, schema, avroVal)
			assert.Equal(t, actualSize, computedSize)
		})

		t.Run(string(qv.Kind())+"_nullable", func(t *testing.T) {
			nullableField := types.QField{Type: qv.Kind(), Nullable: true, Precision: field.Precision, Scale: field.Scale}
			schema := getAvroSchema(t, ctx, env, qv, &nullableField)
			avroVal, computedSize := qvalueToAvro(t, ctx, qv, &nullableField, false)
			actualSize := avroEncodedSize(t, schema, avroVal)
			assert.Equal(t, actualSize, computedSize)
		})

		t.Run(string(qv.Kind())+"_no_calc_size", func(t *testing.T) {
			_, computedSize, err := QValueToAvro(ctx, qv, &field, protos.DBType_CLICKHOUSE,
				log.NewStructuredLogger(nil), false, nil, internal.BinaryFormatRaw, false)
			require.NoError(t, err)
			assert.Equal(t, int64(0), computedSize)
		})
	}
}

// TestBytesToAvroNullVsEmpty checks that a nil []byte inside QValueBytes encodes as empty bytes and only
// QValueNull takes the null branch. Connectors such as MySQL can hand over an empty binary value as a nil
// slice, and twmb/avro treats a pointer to a nil slice as null in a ["null", "bytes"] union.
func TestBytesToAvroNullVsEmpty(t *testing.T) {
	ctx := context.Background()
	env := map[string]string{"PEERDB_CLICKHOUSE_BINARY_FORMAT": "raw"}

	tests := []struct {
		value types.QValue
		name  string
		// wire bytes for the ["null", "bytes"] union, and for plain bytes when nullable is false
		nullable, nonNullable []byte
	}{
		// union branch 1 (bytes), length 0
		{name: "nil slice", value: types.QValueBytes{Val: nil}, nullable: []byte{0x02, 0x00}, nonNullable: []byte{0x00}},
		{name: "empty slice", value: types.QValueBytes{Val: []byte{}}, nullable: []byte{0x02, 0x00}, nonNullable: []byte{0x00}},
		// union branch 1 (bytes), length 1, 0x07
		{name: "one byte", value: types.QValueBytes{Val: []byte{0x07}}, nullable: []byte{0x02, 0x02, 0x07}, nonNullable: []byte{0x02, 0x07}},
		// union branch 0 (null); a non-nullable column has no null to encode
		{name: "SQL NULL", value: types.QValueNull(types.QValueKindBytes), nullable: []byte{0x00}},
	}
	for _, tc := range tests {
		t.Run(tc.name+"_nullable", func(t *testing.T) {
			field := types.QField{Type: types.QValueKindBytes, Nullable: true}
			schema := getAvroSchema(t, ctx, env, types.QValueBytes{}, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, tc.value, &field, false)
			encoded, err := schema.Encode(avroVal)
			require.NoError(t, err)
			assert.Equal(t, tc.nullable, encoded)
			assert.Equal(t, int64(len(encoded)), computedSize)
		})
		if tc.nonNullable == nil {
			continue
		}
		t.Run(tc.name+"_non_nullable", func(t *testing.T) {
			field := types.QField{Type: types.QValueKindBytes}
			schema := getAvroSchema(t, ctx, env, types.QValueBytes{}, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, tc.value, &field, false)
			encoded, err := schema.Encode(avroVal)
			require.NoError(t, err)
			assert.Equal(t, tc.nonNullable, encoded)
			assert.Equal(t, int64(len(encoded)), computedSize)
		})
	}
}

// TestNumericScaleAbovePrecisionToAvro round-trips values of a Postgres NUMERIC(2,4) column, which holds
// -0.0099 to 0.0099, through the widened decimal(4,4) schema.
func TestNumericScaleAbovePrecisionToAvro(t *testing.T) {
	ctx := context.Background()
	env := map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false"}
	field := types.QField{Type: types.QValueKindNumeric, Precision: 2, Scale: 4}
	schema := getAvroSchema(t, ctx, env, types.QValueNumeric{}, &field)
	for _, value := range []string{"0.0099", "-0.0099", "0.0001", "0"} {
		t.Run(value, func(t *testing.T) {
			avroVal, computedSize := qvalueToAvro(t, ctx, types.QValueNumeric{Val: decimal.RequireFromString(value)}, &field, false)
			encoded, err := schema.Encode(avroVal)
			require.NoError(t, err)
			// decimalSize is an estimate that may overestimate by a byte
			assert.GreaterOrEqual(t, computedSize, int64(len(encoded)))
			assert.LessOrEqual(t, computedSize, int64(len(encoded))+1)
			var decoded big.Rat
			_, err = schema.Decode(encoded, &decoded)
			require.NoError(t, err)
			assert.Equal(t, decimal.RequireFromString(value).Rat().String(), decoded.String())
		})
	}
}

// TestTimestampBeyondInt64MicrosToAvro encodes a timestamp Postgres accepts but int64 microseconds since the
// epoch cannot hold, 294276-12-31 12:00:00, alone and in arrays, for ClickHouse. Scalars are clamped to the
// ClickHouse range before encoding; arrays must be too, or the encoder rejects the value and fails the snapshot.
func TestTimestampBeyondInt64MicrosToAvro(t *testing.T) {
	ctx := context.Background()
	env := map[string]string{}
	beyond := time.Date(294276, 12, 31, 12, 0, 0, 0, time.UTC)
	before := time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC) // 4714-11-24 BC, the Postgres minimum
	values := []time.Time{beyond, time.Date(2024, 1, 2, 3, 4, 5, 6000, time.UTC), before}

	tests := []types.QValue{
		types.QValueTimestamp{Val: beyond},
		types.QValueTimestampTZ{Val: beyond},
		types.QValueArrayTimestamp{Val: values},
		types.QValueArrayTimestampTZ{Val: values},
	}
	for _, qv := range tests {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nullable=%t", qv.Kind(), nullable), func(t *testing.T) {
				field := types.QField{Type: qv.Kind(), Nullable: nullable}
				schema := getAvroSchema(t, ctx, env, qv, &field)
				avroVal, computedSize := qvalueToAvro(t, ctx, qv, &field, false)
				encoded, err := schema.Encode(avroVal)
				require.NoError(t, err)
				assert.Equal(t, int64(len(encoded)), computedSize)
			})
		}
	}
}

// TestScalarTimeBeyondInt64MicrosToAvroS3 encodes scalar times for S3, which takes the default branch of
// processGeneralTime. A timestamp beyond int64 microseconds since the epoch is clamped to the Avro maximum, or the
// encoder rejects it and fails the sync. A date is days since the epoch in an int32, which holds every date Postgres
// accepts (up to 5874897 AD), so it is kept as is.
func TestScalarTimeBeyondInt64MicrosToAvroS3(t *testing.T) {
	ctx := context.Background()
	beyond := time.Date(294276, 12, 31, 12, 0, 0, 0, time.UTC)
	avroMax := time.UnixMicro(math.MaxInt64).UTC()
	farDate := time.Date(300000, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		qv       types.QValue
		expected time.Time
	}{
		{types.QValueTimestamp{Val: beyond}, avroMax},
		{types.QValueTimestampTZ{Val: beyond}, avroMax},
		{types.QValueDate{Val: farDate}, farDate},
	}
	for _, tc := range tests {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nullable=%t", tc.qv.Kind(), nullable), func(t *testing.T) {
				field := types.QField{Type: tc.qv.Kind(), Nullable: nullable}
				schemaNode, err := GetAvroSchemaFromQValueKind(ctx, nil, tc.qv.Kind(), protos.DBType_S3, 0, 0)
				require.NoError(t, err)
				if nullable {
					schemaNode = NullableAvroSchema(schemaNode)
				}
				schema, err := schemaNode.Schema()
				require.NoError(t, err)

				avroVal, computedSize, err := QValueToAvro(ctx, tc.qv, &field, protos.DBType_S3,
					log.NewStructuredLogger(nil), false, nil, internal.BinaryFormatRaw, true)
				require.NoError(t, err)
				converted, ok := avroVal.(time.Time)
				require.True(t, ok, "expected time.Time, got %T", avroVal)
				require.True(t, tc.expected.Equal(converted), "expected %s, got %s", tc.expected, converted)

				encoded, err := schema.Encode(avroVal)
				require.NoError(t, err)
				assert.Equal(t, int64(len(encoded)), computedSize)
			})
		}
	}
}

// TestClampToAvroRanges checks that times int64 microseconds since the epoch cannot hold move to the closest
// boundary, in their own location, while times within the range, boundaries included, are kept as they are.
func TestClampToAvroRanges(t *testing.T) {
	minTime := time.UnixMicro(math.MinInt64).UTC()
	maxTime := time.UnixMicro(math.MaxInt64).UTC()
	offset := time.FixedZone("UTC+5", 5*60*60)

	tests := []struct {
		name     string
		input    time.Time
		expected time.Time
	}{
		{"in range", time.Date(2024, 1, 2, 3, 4, 5, 6000, time.UTC), time.Date(2024, 1, 2, 3, 4, 5, 6000, time.UTC)},
		{"epoch", time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC()},
		{"postgres minimum", time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC), time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},
		{"minimum", minTime, minTime},
		{"maximum", maxTime, maxTime},
		{"just below minimum", minTime.Add(-time.Microsecond), minTime},
		{"just above maximum", maxTime.Add(time.Microsecond), maxTime},
		{"far below minimum", time.Date(-300000, 1, 1, 0, 0, 0, 0, time.UTC), minTime},
		{"postgres maximum", time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC), maxTime},
		{"above maximum, other location", time.Date(294276, 12, 31, 12, 0, 0, 0, offset), maxTime.In(offset)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clamped := ClampToAvroRanges(tc.input)
			require.True(t, tc.expected.Equal(clamped), "expected %s, got %s", tc.expected, clamped)
			require.Equal(t, tc.input.Location(), clamped.Location())
			// the clamped time survives the round trip through int64 microseconds
			require.True(t, clamped.Equal(time.UnixMicro(clamped.UnixMicro())), "%s overflows int64 microseconds", clamped)
		})
	}
}

func TestVarIntSize(t *testing.T) {
	ctx := context.Background()
	env := map[string]string{}

	tests := []struct {
		name  string
		value int64
	}{
		{"zero", 0},
		{"small positive", 1},
		{"max 1-byte positive", 63},
		{"min 2-byte positive", 64},
		{"max 2-byte positive", 8191},
		{"min 3-byte positive", 8192},
		{"small negative", -1},
		{"max 1-byte negative", -64},
		{"min 2-byte negative", -65},
		{"max 2-byte negative", -8192},
		{"min 3-byte negative", -8193},
		{"large positive", 1000000000},
		{"large negative", -1000000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qv := types.QValueInt64{Val: tt.value}
			field := types.QField{Type: types.QValueKindInt64, Nullable: false}
			schema := getAvroSchema(t, ctx, env, qv, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, qv, &field, false)
			actualSize := avroEncodedSize(t, schema, avroVal)
			assert.Equal(t, actualSize, computedSize)
		})
	}
}

func TestStringSize(t *testing.T) {
	ctx := context.Background()
	env := map[string]string{}

	tests := []struct {
		name     string
		str      string
		nullable bool
	}{
		{"empty string", "", false},
		{"single char", "a", false},
		{"simple string", "hello world", false},
		{"longer string", string(make([]byte, 127)), false},
		{"with nullable", string(make([]byte, 127)), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qv := types.QValueString{Val: tt.str}
			field := types.QField{Type: types.QValueKindString, Nullable: tt.nullable}
			schema := getAvroSchema(t, ctx, env, qv, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, qv, &field, false)
			actualSize := avroEncodedSize(t, schema, avroVal)
			assert.Equal(t, actualSize, computedSize)
		})
	}
}

func TestFixedArraySize(t *testing.T) {
	ctx := context.Background()
	env := map[string]string{}

	tests := []struct {
		name     string
		qv       types.QValue
		nullable bool
	}{
		{"empty array", types.QValueArrayFloat64{Val: []float64{}}, false},
		{"single element", types.QValueArrayFloat64{Val: []float64{1.0}}, false},
		{"multiple elements", types.QValueArrayFloat64{Val: []float64{1.0, 2.0, 3.0, 4.0, 5.0}}, false},
		{"with nullable", types.QValueArrayFloat32{Val: []float32{1.0, 2.0, 3.0}}, true},
		{"large count", types.QValueArrayBoolean{Val: make([]bool, 100)}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := types.QField{Type: tt.qv.Kind(), Nullable: tt.nullable}
			schema := getAvroSchema(t, ctx, env, tt.qv, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, tt.qv, &field, false)
			actualSize := avroEncodedSize(t, schema, avroVal)
			assert.Equal(t, actualSize, computedSize)
		})
	}
}

func TestStringArraySize(t *testing.T) {
	ctx := context.Background()
	env := map[string]string{}

	tests := []struct {
		name     string
		vals     []string
		nullable bool
	}{
		{"empty array", []string{}, false},
		{"single elem", []string{"a"}, false},
		{"multiple elems", []string{"", "ab", "xyz"}, false},
		{"with nullable", []string{"test"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qv := types.QValueArrayString{Val: tt.vals}
			field := types.QField{Type: types.QValueKindArrayString, Nullable: tt.nullable}
			schema := getAvroSchema(t, ctx, env, qv, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, qv, &field, false)
			actualSize := avroEncodedSize(t, schema, avroVal)
			assert.Equal(t, actualSize, computedSize)
		})
	}
}

func TestDecimalSize(t *testing.T) {
	tests := []struct {
		name      string
		num       decimal.Decimal
		precision int16
		scale     int16
		nullable  bool
	}{
		// Zero
		{"zero", decimal.NewFromInt(0), 76, 38, false},
		{"zero_nullable", decimal.NewFromInt(0), 76, 38, true},

		// Small integers (single byte range)
		{"one", decimal.NewFromInt(1), 76, 38, false},
		{"neg_one", decimal.NewFromInt(-1), 76, 38, false},
		{"max_int8", decimal.NewFromInt(127), 76, 38, false},
		{"min_int8", decimal.NewFromInt(-128), 76, 38, false},

		// Two-byte boundary
		{"min_int8+1", decimal.NewFromInt(128), 76, 38, false},
		{"max_int8-1", decimal.NewFromInt(-129), 76, 38, false},
		{"max_int16", decimal.NewFromInt(32767), 76, 38, false},
		{"min_int16", decimal.NewFromInt(-32768), 76, 38, false},

		// Many digits
		{"1000000", decimal.NewFromInt(1000000), 76, 38, false},
		{"-1000000", decimal.NewFromInt(-1000000), 76, 38, false},
		{"10_digits", decimal.NewFromInt(9999999999), 76, 38, false},
		{"37_nines", decimal.RequireFromString(strings.Repeat("9", 37)), 76, 38, false},
		{"38_nines", decimal.RequireFromString(strings.Repeat("9", 38)), 76, 38, false},
		{"100_nines", decimal.RequireFromString(strings.Repeat("9", 100)), 76, 38, false},
		{"1000_nines", decimal.RequireFromString(strings.Repeat("9", 1000)), 76, 38, false},
		{"10000_nines", decimal.RequireFromString(strings.Repeat("9", 10000)), 76, 38, false},
		{"100000_nines", decimal.RequireFromString(strings.Repeat("9", 100000)), 76, 38, false},

		// Decimals with scale
		{"1.5", decimal.NewFromFloat(1.5), 76, 38, false},
		{"-1.5", decimal.NewFromFloat(-1.5), 76, 38, false},
		{"123.456", decimal.NewFromFloat(123.456), 76, 38, false},
		{"0.001", decimal.NewFromFloat(0.001), 76, 38, false},
		{"99999.99999", decimal.NewFromFloat(99999.99999), 76, 38, false},
		{"max_pg_decimal", decimal.RequireFromString(strings.Repeat("9", 131072) + "." + strings.Repeat("9", 16384)), 76, 38, false},
		{"min_pg_decimal", decimal.RequireFromString("-" + strings.Repeat("9", 131072) + "." + strings.Repeat("9", 16384)), 76, 38, false},

		// Nullable
		{"123.45_nullable", decimal.NewFromFloat(123.45), 76, 38, true},
		{"-999_nullable", decimal.NewFromInt(-999), 76, 38, true},
	}

	ctx := context.Background()
	env := map[string]string{
		"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qv := types.QValueNumeric{Val: tt.num, Precision: tt.precision, Scale: tt.scale}
			field := types.QField{Type: types.QValueKindNumeric, Nullable: tt.nullable, Precision: tt.precision, Scale: tt.scale}
			schema := getAvroSchema(t, ctx, env, qv, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, qv, &field, false)
			actualSize := avroEncodedSize(t, schema, avroVal)
			// computedSize may overestimate by up to 1 byte due to log2(10) estimation rounding
			assert.True(t, computedSize == actualSize || computedSize == actualSize+1,
				"computedSize %d should equal actualSize %d or actualSize+1", computedSize, actualSize)
		})
	}

	stringTests := []decimal.Decimal{
		decimal.NewFromFloat(123.45),
		decimal.NewFromInt(0),
		decimal.NewFromFloat(-999.999),
	}

	stringEnv := map[string]string{
		"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "true",
	}

	for _, num := range stringTests {
		t.Run("string/"+num.String(), func(t *testing.T) {
			qv := types.QValueNumeric{Val: num}
			field := types.QField{Type: types.QValueKindNumeric, Nullable: false}
			schema := getAvroSchema(t, ctx, stringEnv, qv, &field)
			avroVal, computedSize := qvalueToAvro(t, ctx, qv, &field, true)
			actualSize := avroEncodedSize(t, schema, avroVal)
			assert.Equal(t, actualSize, computedSize)
		})
	}
}

func getAvroSchema(
	t *testing.T, ctx context.Context, env map[string]string, qv types.QValue, field *types.QField,
) *avro.Schema {
	t.Helper()
	schema, err := GetAvroSchemaFromQValueKind(ctx, env, qv.Kind(), protos.DBType_CLICKHOUSE, field.Precision, field.Scale)
	require.NoError(t, err)
	if field.Nullable {
		schema = NullableAvroSchema(schema)
	}
	avroSchema, err := schema.Schema()
	require.NoError(t, err)
	return avroSchema
}

func qvalueToAvro(
	t *testing.T, ctx context.Context, qv types.QValue, field *types.QField, unboundedNumericAsString bool,
) (any, int64) {
	t.Helper()
	avroVal, computedSize, err := QValueToAvro(ctx, qv, field, protos.DBType_CLICKHOUSE,
		log.NewStructuredLogger(nil), unboundedNumericAsString, nil, internal.BinaryFormatRaw, true)
	require.NoError(t, err)
	return avroVal, computedSize
}

func avroEncodedSize(t *testing.T, schema *avro.Schema, value any) int64 {
	t.Helper()
	data, err := schema.Encode(value)
	require.NoError(t, err)
	return int64(len(data))
}
