package connmongo

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
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
		{
			desc:     "uint64",
			input:    bson.D{{Key: "a", Value: uint64(18446744073709551615)}},
			expected: `{"a":18446744073709551615}`,
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
			desc:     "bson.A special float",
			input:    bson.D{{Key: "a", Value: bson.A{math.NaN(), math.Inf(1), math.Inf(-1)}}},
			expected: `{"a":["NaN","+Inf","-Inf"]}`,
		},
		{
			desc: "bson.A nested special float",
			input: bson.D{{Key: "nested", Value: bson.A{
				bson.D{{Key: "inner1", Value: bson.A{
					bson.D{{Key: "inner_inner", Value: bson.A{
						math.NaN(), math.Inf(1), math.Inf(-1),
					}}},
				}}},
				bson.D{{Key: "inner2", Value: 1.23}},
			}}},
			expected: `{"nested":[{"inner1":[{"inner_inner":["NaN","+Inf","-Inf"]}]},{"inner2":1.23}]}`,
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
			desc:     "bson.DateTime",
			input:    bson.D{{Key: "date", Value: bson.DateTime(1672531200000)}}, // 2023-01-01 00:00:00 UTC
			expected: `{"date":"2023-01-01T00:00:00Z"}`,
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

		{
			desc:     "bson.Null",
			input:    bson.D{{Key: "null", Value: bson.Null{}}},
			expected: `{"null":{}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			result, err := API.Marshal(test.input)
			require.NoError(t, err, "Failed to marshal %s", test.desc)
			require.Equal(t, test.expected, string(result), "Unexpected result for %s", test.desc)
		})
	}
}

func TestMarshalId(t *testing.T) {
	objectId, err := bson.ObjectIDFromHex("6893edbecb1f9508891bbb84")
	require.NoError(t, err)
	result, err := API.Marshal(objectId)
	require.NoError(t, err)
	require.Equal(t, `"6893edbecb1f9508891bbb84"`, string(result))
}

// Tests that floats of all magnitudes are marshalled into a resonable length and have a signifier
// that they're not integers
func TestMarshalFloatLengths(t *testing.T) {
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
				result, err := API.Marshal(input)
				require.NoError(t, err)
				resultStr := string(result)
				require.Less(t, len(resultStr), 33)
				require.True(t,
					strings.Contains(resultStr, ".") ||
						strings.Contains(resultStr, "e") ||
						strings.Contains(resultStr, "Inf"),
					resultStr)
			})
		}
	}
}
