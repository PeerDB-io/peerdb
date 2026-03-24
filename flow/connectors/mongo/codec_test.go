package connmongo

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TestMarshalDocument(t *testing.T) {
	tests := []struct {
		desc     string
		expected string
		input    bson.D
	}{
		// String types
		{
			desc:     "String",
			input:    bson.D{{Key: "a", Value: "hello"}},
			expected: `{"a":"hello"}`,
		},
		{
			desc:     "Empty string",
			input:    bson.D{{Key: "a", Value: ""}},
			expected: `{"a":""}`,
		},
		{
			desc:     "String with special chars",
			input:    bson.D{{Key: "a", Value: "hello\nworld\t\"quoted\""}},
			expected: `{"a":"hello\nworld\t\"quoted\""}`,
		},

		// Boolean type
		{
			desc:     "Boolean true",
			input:    bson.D{{Key: "a", Value: true}},
			expected: `{"a":true}`,
		},
		{
			desc:     "Boolean false",
			input:    bson.D{{Key: "a", Value: false}},
			expected: `{"a":false}`,
		},

		// Integer types
		{
			desc:     "int",
			input:    bson.D{{Key: "a", Value: 42}},
			expected: `{"a":42}`,
		},
		{
			desc:     "int8",
			input:    bson.D{{Key: "a", Value: int8(127)}},
			expected: `{"a":127}`,
		},
		{
			desc:     "int16",
			input:    bson.D{{Key: "a", Value: int16(32767)}},
			expected: `{"a":32767}`,
		},
		{
			desc:     "int32",
			input:    bson.D{{Key: "a", Value: int32(2147483647)}},
			expected: `{"a":2147483647}`,
		},
		{
			desc:     "int64",
			input:    bson.D{{Key: "a", Value: int64(9223372036854775807)}},
			expected: `{"a":9223372036854775807}`,
		},
		{
			desc:     "uint",
			input:    bson.D{{Key: "a", Value: uint(42)}},
			expected: `{"a":42}`,
		},
		{
			desc:     "uint8",
			input:    bson.D{{Key: "a", Value: uint8(255)}},
			expected: `{"a":255}`,
		},
		{
			desc:     "uint16",
			input:    bson.D{{Key: "a", Value: uint16(65535)}},
			expected: `{"a":65535}`,
		},
		{
			desc:     "uint32",
			input:    bson.D{{Key: "a", Value: uint32(4294967295)}},
			expected: `{"a":4294967295}`,
		},
		// Negative integers
		{
			desc:     "negative int",
			input:    bson.D{{Key: "a", Value: int(-42)}},
			expected: `{"a":-42}`,
		},
		{
			desc:     "negative int8",
			input:    bson.D{{Key: "a", Value: int8(-128)}},
			expected: `{"a":-128}`,
		},
		{
			desc:     "negative int16",
			input:    bson.D{{Key: "a", Value: int16(-32768)}},
			expected: `{"a":-32768}`,
		},
		{
			desc:     "negative int32",
			input:    bson.D{{Key: "a", Value: int32(-2147483648)}},
			expected: `{"a":-2147483648}`,
		},
		{
			desc:     "negative int64",
			input:    bson.D{{Key: "a", Value: int64(-9223372036854775808)}},
			expected: `{"a":-9223372036854775808}`,
		},

		// Floating point types - normal values
		{
			desc:     "float64",
			input:    bson.D{{Key: "a", Value: float64(3.14159265359)}},
			expected: `{"a":3.14159265359}`,
		},
		{
			desc:     "negative float64",
			input:    bson.D{{Key: "a", Value: float64(-3.14159265359)}},
			expected: `{"a":-3.14159265359}`,
		},
		{
			desc:     "float64 0 fractional value",
			input:    bson.D{{Key: "a", Value: float64(3)}},
			expected: `{"a":3.0}`,
		},
		{
			desc:     "float64 max int64",
			input:    bson.D{{Key: "a", Value: float64(math.MaxInt64)}},
			expected: `{"a":9223372036854775808.0}`,
		},
		{
			desc:     "float64 min int64",
			input:    bson.D{{Key: "a", Value: float64(math.MinInt64)}},
			expected: `{"a":-9223372036854775808.0}`,
		},
		{
			desc:     "float64 greater than max int64",
			input:    bson.D{{Key: "a", Value: math.Pow(2, 65)}},
			expected: `{"a":36893488147419103232.0}`,
		},
		{
			desc:     "float64 less than min int64",
			input:    bson.D{{Key: "a", Value: -math.Pow(2, 65)}},
			expected: `{"a":-36893488147419103232.0}`,
		},

		// Special float values - NaN and Infinity (these are handled by our custom codec)
		{
			desc:     "float64 NaN",
			input:    bson.D{{Key: "a", Value: math.NaN()}},
			expected: `{"a":"NaN"}`,
		},
		{
			desc:     "float64 +Inf",
			input:    bson.D{{Key: "a", Value: math.Inf(1)}},
			expected: `{"a":"+Inf"}`,
		},
		{
			desc:     "float64 -Inf",
			input:    bson.D{{Key: "a", Value: math.Inf(-1)}},
			expected: `{"a":"-Inf"}`,
		},

		// DateTime
		{
			desc:     "bson.DateTime",
			input:    bson.D{{Key: "date", Value: bson.DateTime(1672531200000)}}, // 2023-01-01 00:00:00 UTC
			expected: `{"date":"2023-01-01T00:00:00Z"}`,
		},
		{
			desc:     "bson.DateTime out of RFC3339 range",
			input:    bson.D{{Key: "date", Value: bson.DateTime(253433923200000)}}, // 10001-01-01 00:00:00 UTC
			expected: `{"date":"10001-01-01T00:00:00Z"}`,
		},
		{
			desc:     "bson.DateTime max",
			input:    bson.D{{Key: "date", Value: bson.DateTime(math.MaxInt64)}},
			expected: `{"date":"292278994-08-17T07:12:55.807Z"}`,
		},
		{
			desc:     "bson.DateTime max",
			input:    bson.D{{Key: "date", Value: bson.DateTime(math.MinInt64)}},
			expected: `{"date":"-292275055-05-16T16:47:04.192Z"}`,
		},

		// Null values
		{
			desc:     "nil",
			input:    bson.D{{Key: "a", Value: nil}},
			expected: `{"a":null}`,
		},

		// Multiple fields
		{
			desc: "multiple fields",
			input: bson.D{
				{Key: "str", Value: "hello"},
				{Key: "num", Value: 42},
				{Key: "bool", Value: true},
				{Key: "null", Value: nil},
			},
			expected: `{"str":"hello","num":42,"bool":true,"null":null}`,
		},

		// nested bson.D
		{
			desc: "nested bson.D",
			input: bson.D{{Key: "nested", Value: bson.D{
				{Key: "inner1", Value: "str"},
				{Key: "inner2", Value: 1},
				{Key: "inner3", Value: true},
				{
					Key: "inner4", Value: bson.D{
						{Key: "a", Value: math.NaN()},
						{Key: "b", Value: []string{"hello", "world"}},
					},
				},
			}}},
			expected: `{"nested":{"inner1":"str","inner2":1,"inner3":true,"inner4":{"a":"NaN","b":["hello","world"]}}}`,
		},

		// nested bson.A
		{
			desc:     "bson.A",
			input:    bson.D{{Key: "a", Value: bson.A{1, "str", true}}},
			expected: `{"a":[1,"str",true]}`,
		},
		{
			desc:     "bson.A special values",
			input:    bson.D{{Key: "a", Value: bson.A{math.NaN(), math.Inf(1), math.Inf(-1), bson.DateTime(math.MaxInt64)}}},
			expected: `{"a":["NaN","+Inf","-Inf","292278994-08-17T07:12:55.807Z"]}`,
		},
		{
			desc: "bson.A nested special values",
			input: bson.D{{Key: "nested", Value: bson.A{
				bson.D{{Key: "inner1", Value: bson.A{
					bson.D{{Key: "inner_inner", Value: bson.A{
						math.NaN(), math.Inf(1), math.Inf(-1), bson.DateTime(math.MaxInt64),
					}}},
				}}},
				bson.D{{Key: "inner2", Value: 1.23}},
			}}},
			expected: `{"nested":[{"inner1":[{"inner_inner":["NaN","+Inf","-Inf","292278994-08-17T07:12:55.807Z"]}]},{"inner2":1.23}]}`,
		},
		{
			desc: "complex nested bson.A",
			input: bson.D{{Key: "complex", Value: bson.A{
				bson.D{{Key: "NaN", Value: math.NaN()}},
				bson.D{{Key: "binary", Value: bson.Binary{Subtype: 0x00, Data: []byte("test")}}},
				bson.D{{
					Key:   "nested_arr",
					Value: bson.A{bson.A{1}, bson.A{2}, bson.A{3}},
				}},
				bson.D{{
					Key:   "nested_doc",
					Value: bson.D{{Key: "str", Value: "hello world"}},
				}},
				bson.D{{Key: "timestamp", Value: bson.Timestamp{T: 1672531200, I: 1}}},
			}}},
			expected: `{"complex":[{"NaN":"NaN"},{"binary":{"Subtype":0,"Data":"dGVzdA=="}},{"nested_arr":[[1],[2],[3]]},` +
				`{"nested_doc":{"str":"hello world"}},{"timestamp":{"T":1672531200,"I":1}}]}`,
		},
		{
			desc: "bson.A mixed",
			input: bson.D{{Key: "mixed", Value: bson.A{
				3.14,
				bson.D{{Key: "num", Value: 1}},
			}}},
			expected: `{"mixed":[3.14,{"num":1}]}`,
		},
		// other bson types
		{
			desc: "bson.ObjectID",
			input: func() bson.D {
				oid, _ := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
				return bson.D{{Key: "id", Value: oid}}
			}(),
			expected: `{"id":"507f1f77bcf86cd799439011"}`,
		},
		{
			desc:     "bson.JavaScript",
			input:    bson.D{{Key: "js", Value: bson.JavaScript("function() { return 42; }")}},
			expected: `{"js":"function() { return 42; }"}`,
		},
		{
			desc:     "bson.Symbol",
			input:    bson.D{{Key: "symbol", Value: bson.Symbol("test_symbol")}},
			expected: `{"symbol":"test_symbol"}`,
		},
		{
			desc:  "bson.Binary",
			input: bson.D{{Key: "binary", Value: bson.Binary{Subtype: 0x02, Data: []byte("hello world")}}},
			expected: fmt.Sprintf(`{"binary":{"Subtype":2,"Data":"%s"}}`,
				base64.StdEncoding.EncodeToString([]byte("hello world"))),
		},
		{
			desc:     "bson.Binary empty",
			input:    bson.D{{Key: "binary", Value: bson.Binary{Subtype: 0x00, Data: []byte{}}}},
			expected: `{"binary":{"Subtype":0,"Data":""}}`,
		},
		{
			desc:     "bson.Timestamp",
			input:    bson.D{{Key: "ts", Value: bson.Timestamp{T: 1672531200, I: 1}}},
			expected: `{"ts":{"T":1672531200,"I":1}}`,
		},
		{
			desc:     "bson.Regex",
			input:    bson.D{{Key: "regex", Value: bson.Regex{Pattern: "^test.*", Options: "i"}}},
			expected: `{"regex":{"Pattern":"^test.*","Options":"i"}}`,
		},
		{
			desc: "bson.Decimal128",
			input: func() bson.D {
				dec, _ := bson.ParseDecimal128("123.4567890987654321")
				return bson.D{{Key: "decimal", Value: dec}}
			}(),
			expected: `{"decimal":"123.4567890987654321"}`,
		},
		{
			desc:     "bson.Undefined",
			input:    bson.D{{Key: "undefined", Value: bson.Undefined{}}},
			expected: `{"undefined":{}}`,
		},
	}

	converter := NewDirectBsonConverter()
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			inputRaw, err := bson.Marshal(test.input)
			require.NoError(t, err)
			result, err := converter.QValueJSONFromDocument(inputRaw)
			require.NoError(t, err)
			require.Equal(t, test.expected, result.Val, "Unexpected result for %s", test.desc)
		})
	}
}

func TestMarshalId(t *testing.T) {
	toIdRaw := func(val any) bson.Raw {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: val}})
		require.NoError(t, err)
		return raw
	}
	converter := NewDirectBsonConverter()

	objectId, err := bson.ObjectIDFromHex("6893edbecb1f9508891bbb84")
	require.NoError(t, err)
	objectIdRaw := toIdRaw(objectId)

	// Test new version behavior (no redundant quotes for ObjectID and string)
	qValue, err := converter.QValueStringFromKey(objectIdRaw, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, `6893edbecb1f9508891bbb84`, qValue.Val)

	stringIdRaw := toIdRaw("string_pk")
	qValue, err = converter.QValueStringFromKey(stringIdRaw, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, `string_pk`, qValue.Val)

	intIdRaw := toIdRaw(123)
	qValue, err = converter.QValueStringFromKey(intIdRaw, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, `123`, qValue.Val)

	compositeIdRaw := toIdRaw(bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 2}})
	qValue, err = converter.QValueStringFromKey(compositeIdRaw, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, `{"a":1,"b":2}`, qValue.Val)

	// Test old version behavior (JSON quoting preserved for backwards compatibility)
	qValue, err = converter.QValueStringFromKey(objectIdRaw, shared.InternalVersion_First)
	require.NoError(t, err)
	require.Equal(t, `"6893edbecb1f9508891bbb84"`, qValue.Val)

	qValue, err = converter.QValueStringFromKey(stringIdRaw, shared.InternalVersion_First)
	require.NoError(t, err)
	require.Equal(t, `"string_pk"`, qValue.Val)
}

// TestRawDocToJSON_MatchesOldPath verifies that DirectBsonConverter produces byte-identical output
// to the LegacyBsonConverter (bson.Unmarshal + json-iterator) path for every BSON data type.
// TODO: remove when LegacyBsonConverter is deleted
func TestRawDocToJSON_MatchesOldPath(t *testing.T) {
	oid, _ := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	dec, _ := bson.ParseDecimal128("123.4567890987654321")
	decNaN, _ := bson.ParseDecimal128("NaN")
	decInf, _ := bson.ParseDecimal128("Infinity")

	tests := []struct {
		desc  string
		input bson.D
	}{
		// --- TypeString (0x02) ---
		{desc: "string/plain", input: bson.D{{Key: "a", Value: "hello"}}},
		{desc: "string/empty", input: bson.D{{Key: "a", Value: ""}}},
		{desc: "string/special_chars", input: bson.D{{Key: "a", Value: "hello\nworld\t\"quoted\"\r\nend"}}},
		{desc: "string/backslash", input: bson.D{{Key: "a", Value: `back\slash`}}},
		{desc: "string/control_chars", input: bson.D{{Key: "a", Value: "\x00\x01\x02\x1f"}}},
		{desc: "string/html_entities", input: bson.D{{Key: "a", Value: "a&b<c>d"}}},
		{desc: "string/html_script", input: bson.D{{Key: "a", Value: "<script>alert('xss')</script>"}}},
		{desc: "string/unicode_line_separators", input: bson.D{{Key: "a", Value: "line\u2028sep\u2029par"}}},
		{desc: "string/unicode_multibyte", input: bson.D{{Key: "a", Value: "日本語テスト"}}},
		{desc: "string/emoji", input: bson.D{{Key: "a", Value: "hello 🌍🚀"}}},

		// --- TypeBoolean (0x08) ---
		{desc: "bool/true", input: bson.D{{Key: "a", Value: true}}},
		{desc: "bool/false", input: bson.D{{Key: "a", Value: false}}},

		// --- TypeInt32 (0x10) ---
		{desc: "int32/zero", input: bson.D{{Key: "a", Value: int32(0)}}},
		{desc: "int32/positive", input: bson.D{{Key: "a", Value: int32(2147483647)}}},
		{desc: "int32/negative", input: bson.D{{Key: "a", Value: int32(-2147483648)}}},

		// --- TypeInt64 (0x12) ---
		{desc: "int64/zero", input: bson.D{{Key: "a", Value: int64(0)}}},
		{desc: "int64/max", input: bson.D{{Key: "a", Value: int64(math.MaxInt64)}}},
		{desc: "int64/min", input: bson.D{{Key: "a", Value: int64(math.MinInt64)}}},

		// --- TypeDouble (0x01) ---
		{desc: "double/normal", input: bson.D{{Key: "a", Value: float64(3.14159265359)}}},
		{desc: "double/negative", input: bson.D{{Key: "a", Value: float64(-3.14159265359)}}},
		{desc: "double/integer_valued", input: bson.D{{Key: "a", Value: float64(42)}}},
		{desc: "double/zero", input: bson.D{{Key: "a", Value: float64(0)}}},
		{desc: "double/negative_zero", input: bson.D{{Key: "a", Value: math.Copysign(0, -1)}}},
		{desc: "double/very_small", input: bson.D{{Key: "a", Value: 1e-7}}},
		{desc: "double/very_large", input: bson.D{{Key: "a", Value: 1e21}}},
		{desc: "double/max_int64_as_float", input: bson.D{{Key: "a", Value: float64(math.MaxInt64)}}},
		{desc: "double/NaN", input: bson.D{{Key: "a", Value: math.NaN()}}},
		{desc: "double/+Inf", input: bson.D{{Key: "a", Value: math.Inf(1)}}},
		{desc: "double/-Inf", input: bson.D{{Key: "a", Value: math.Inf(-1)}}},

		// --- TypeNull (0x0A) ---
		{desc: "null/nil", input: bson.D{{Key: "a", Value: nil}}},
		{desc: "null/bson.Null", input: bson.D{{Key: "a", Value: bson.Null{}}}},

		// --- TypeObjectID (0x07) ---
		{desc: "objectid", input: bson.D{{Key: "id", Value: oid}}},

		// --- TypeDateTime (0x09) ---
		{desc: "datetime/epoch", input: bson.D{{Key: "d", Value: bson.DateTime(0)}}},
		{desc: "datetime/normal", input: bson.D{{Key: "d", Value: bson.DateTime(1672531200000)}}},
		{desc: "datetime/max", input: bson.D{{Key: "d", Value: bson.DateTime(math.MaxInt64)}}},
		{desc: "datetime/min", input: bson.D{{Key: "d", Value: bson.DateTime(math.MinInt64)}}},
		{desc: "datetime/with_millis", input: bson.D{{Key: "d", Value: bson.DateTime(1672531200123)}}},

		// --- TypeDecimal128 (0x13) ---
		{desc: "decimal128/normal", input: bson.D{{Key: "d", Value: dec}}},
		{desc: "decimal128/NaN", input: bson.D{{Key: "d", Value: decNaN}}},
		{desc: "decimal128/Infinity", input: bson.D{{Key: "d", Value: decInf}}},

		// --- TypeBinary (0x05) ---
		{desc: "binary/generic", input: bson.D{{Key: "b", Value: bson.Binary{Subtype: 0x00, Data: []byte("test")}}}},
		{desc: "binary/uuid", input: bson.D{{Key: "b", Value: bson.Binary{Subtype: 0x04, Data: []byte("0123456789abcdef")}}}},
		{desc: "binary/empty", input: bson.D{{Key: "b", Value: bson.Binary{Subtype: 0x00, Data: []byte{}}}}},

		// --- TypeTimestamp (0x11) ---
		{desc: "timestamp", input: bson.D{{Key: "ts", Value: bson.Timestamp{T: 1672531200, I: 1}}}},
		{desc: "timestamp/zero", input: bson.D{{Key: "ts", Value: bson.Timestamp{T: 0, I: 0}}}},

		// --- TypeRegex (0x0B) ---
		{desc: "regex", input: bson.D{{Key: "r", Value: bson.Regex{Pattern: "^test.*", Options: "i"}}}},
		{desc: "regex/empty_options", input: bson.D{{Key: "r", Value: bson.Regex{Pattern: ".*", Options: ""}}}},

		// --- TypeJavaScript (0x0D) ---
		{desc: "javascript", input: bson.D{{Key: "js", Value: bson.JavaScript("function() { return 42; }")}}},

		// --- TypeSymbol (0x0E) ---
		{desc: "symbol", input: bson.D{{Key: "s", Value: bson.Symbol("test_symbol")}}},

		// --- TypeUndefined (0x06) ---
		{desc: "undefined", input: bson.D{{Key: "u", Value: bson.Undefined{}}}},

		// --- TypeEmbeddedDocument (0x03) ---
		{desc: "nested_doc/empty", input: bson.D{{Key: "d", Value: bson.D{}}}},
		{desc: "nested_doc/simple", input: bson.D{{Key: "d", Value: bson.D{
			{Key: "x", Value: int32(1)}, {Key: "y", Value: "two"},
		}}}},
		{desc: "nested_doc/deep", input: bson.D{{Key: "a", Value: bson.D{
			{Key: "b", Value: bson.D{{Key: "c", Value: bson.D{{Key: "d", Value: int32(42)}}}}},
		}}}},

		// --- TypeArray (0x04) ---
		{desc: "array/empty", input: bson.D{{Key: "a", Value: bson.A{}}}},
		{desc: "array/mixed_types", input: bson.D{{Key: "a", Value: bson.A{
			int32(1), "two", true, nil, 3.14,
		}}}},
		{desc: "array/nested_arrays", input: bson.D{{Key: "a", Value: bson.A{
			bson.A{int32(1), int32(2)}, bson.A{int32(3), int32(4)},
		}}}},

		// --- Nested special values (BsonExtension recursion) ---
		{desc: "nested/NaN_in_doc", input: bson.D{{Key: "d", Value: bson.D{{Key: "x", Value: math.NaN()}}}}},
		{desc: "nested/Inf_in_array", input: bson.D{{Key: "a", Value: bson.A{math.Inf(1), math.Inf(-1)}}}},
		{desc: "nested/DateTime_in_array", input: bson.D{{Key: "a", Value: bson.A{bson.DateTime(1672531200000)}}}},
		{desc: "nested/specials_deep", input: bson.D{{Key: "d", Value: bson.D{
			{Key: "arr", Value: bson.A{
				bson.D{{Key: "v", Value: math.NaN()}},
				bson.D{{Key: "v", Value: bson.DateTime(0)}},
				bson.D{{Key: "v", Value: math.Inf(-1)}},
			}},
		}}}},

		// --- Multi-field document ---
		{desc: "realistic_document", input: bson.D{
			{Key: "_id", Value: oid},
			{Key: "name", Value: "John Doe"},
			{Key: "email", Value: "john@example.com"},
			{Key: "age", Value: int32(30)},
			{Key: "balance", Value: 1234.56},
			{Key: "active", Value: true},
			{Key: "created", Value: bson.DateTime(1672531200000)},
			{Key: "tags", Value: bson.A{"admin", "user"}},
			{Key: "address", Value: bson.D{
				{Key: "street", Value: "123 Main St"},
				{Key: "city", Value: "Springfield"},
				{Key: "zip", Value: int32(12345)},
			}},
			{Key: "score", Value: dec},
			{Key: "notes", Value: nil},
		}},
	}

	rawConverter := NewDirectBsonConverter()
	legacyConverter := NewLegacyBsonConverter()

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			raw, err := bson.Marshal(test.input)
			require.NoError(t, err)
			directResult, err := rawConverter.QValueJSONFromDocument(raw)
			require.NoError(t, err)

			legacyResult, err := legacyConverter.QValueJSONFromDocument(raw)
			require.NoError(t, err)

			require.Equal(t, legacyResult, directResult)
		})
	}
}

// Tests that floats of all magnitudes are marshaled into a reasonable length and have a signifier
// that they're not integers
func TestMarshalFloatLengths(t *testing.T) {
	converter := NewDirectBsonConverter()
	maxExponent := 309
	require.Equal(t, math.Inf(1), math.Pow10(maxExponent), "exponent range should cover +Inf")   //nolint:testifylint
	require.Equal(t, math.Inf(-1), -math.Pow10(maxExponent), "exponent range should cover -Inf") //nolint:testifylint
	for _, negative := range []bool{false, true} {
		for exponent := range maxExponent + 1 {
			value := math.Pow10(exponent)
			if negative {
				value = -value
			}
			name := fmt.Sprint(value)
			t.Run(name, func(t *testing.T) {
				input := bson.D{{Key: "a", Value: value}}
				raw, err := bson.Marshal(input)
				require.NoError(t, err)
				result, err := converter.QValueJSONFromDocument(raw)
				require.NoError(t, err)
				require.Less(t, len(result.Val), 33)
				require.True(t,
					strings.Contains(result.Val, ".") ||
						strings.Contains(result.Val, "e") ||
						strings.Contains(result.Val, "Inf"),
					result.Val)
			})
		}
	}

	// Test the boundary around the limit itself
	for _, value := range []float64{
		floatLimit, math.Nextafter(floatLimit, math.Inf(1)), math.Nextafter(floatLimit, math.Inf(-1)),
		floatNegLimit, math.Nextafter(floatNegLimit, math.Inf(1)), math.Nextafter(floatNegLimit, math.Inf(-1)),
	} {
		name := fmt.Sprint(value)
		t.Run(name, func(t *testing.T) {
			input := bson.D{{Key: "a", Value: value}}
			raw, err := bson.Marshal(input)
			require.NoError(t, err)
			result, err := converter.QValueJSONFromDocument(raw)
			require.NoError(t, err)
			require.Less(t, len(result.Val), 33)
			require.True(t,
				strings.Contains(result.Val, ".") ||
					strings.Contains(result.Val, "e"),
				result.Val)
		})
	}
}
