package qvalue

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
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

	//nolint:govet // intended for readability
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
) avro.Schema {
	t.Helper()
	schema, err := GetAvroSchemaFromQValueKind(ctx, env, qv.Kind(), protos.DBType_CLICKHOUSE, field.Precision, field.Scale)
	require.NoError(t, err)
	if field.Nullable {
		schema, err = NullableAvroSchema(schema)
		require.NoError(t, err)
	}
	return schema
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

func avroEncodedSize(t *testing.T, schema avro.Schema, value any) int64 {
	t.Helper()
	data, err := avro.Marshal(schema, value)
	require.NoError(t, err)
	return int64(len(data))
}
