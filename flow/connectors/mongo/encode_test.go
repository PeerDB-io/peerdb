package connmongo

import (
	"math"
	"testing"
	"time"

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
			expected: "{\"a\":\"hello\"}",
		},
		{
			desc:     "Empty string",
			input:    bson.D{{Key: "a", Value: ""}},
			expected: "{\"a\":\"\"}",
		},
		{
			desc:     "String with special chars",
			input:    bson.D{{Key: "a", Value: "hello\nworld\t\"quoted\""}},
			expected: "{\"a\":\"hello\\nworld\\t\\\"quoted\\\"\"}",
		},

		// Boolean type
		{
			desc:     "Boolean true",
			input:    bson.D{{Key: "a", Value: true}},
			expected: "{\"a\":true}",
		},
		{
			desc:     "Boolean false",
			input:    bson.D{{Key: "a", Value: false}},
			expected: "{\"a\":false}",
		},

		// Integer types
		{
			desc:     "int",
			input:    bson.D{{Key: "a", Value: 42}},
			expected: "{\"a\":42}",
		},
		{
			desc:     "int8",
			input:    bson.D{{Key: "a", Value: int8(127)}},
			expected: "{\"a\":127}",
		},
		{
			desc:     "int16",
			input:    bson.D{{Key: "a", Value: int16(32767)}},
			expected: "{\"a\":32767}",
		},
		{
			desc:     "int32",
			input:    bson.D{{Key: "a", Value: int32(2147483647)}},
			expected: "{\"a\":2147483647}",
		},
		{
			desc:     "int64",
			input:    bson.D{{Key: "a", Value: int64(9223372036854775807)}},
			expected: "{\"a\":9223372036854775807}",
		},
		{
			desc:     "uint",
			input:    bson.D{{Key: "a", Value: uint(42)}},
			expected: "{\"a\":42}",
		},
		{
			desc:     "uint8",
			input:    bson.D{{Key: "a", Value: uint8(255)}},
			expected: "{\"a\":255}",
		},
		{
			desc:     "uint16",
			input:    bson.D{{Key: "a", Value: uint16(65535)}},
			expected: "{\"a\":65535}",
		},
		{
			desc:     "uint32",
			input:    bson.D{{Key: "a", Value: uint32(4294967295)}},
			expected: "{\"a\":4294967295}",
		},
		{
			desc:     "uint64",
			input:    bson.D{{Key: "a", Value: uint64(18446744073709551615)}},
			expected: "{\"a\":18446744073709551615}",
		},

		// Negative integers
		{
			desc:     "negative int",
			input:    bson.D{{Key: "a", Value: int(-42)}},
			expected: "{\"a\":-42}",
		},
		{
			desc:     "negative int8",
			input:    bson.D{{Key: "a", Value: int8(-128)}},
			expected: "{\"a\":-128}",
		},
		{
			desc:     "negative int16",
			input:    bson.D{{Key: "a", Value: int16(-32768)}},
			expected: "{\"a\":-32768}",
		},
		{
			desc:     "negative int32",
			input:    bson.D{{Key: "a", Value: int32(-2147483648)}},
			expected: "{\"a\":-2147483648}",
		},
		{
			desc:     "negative int64",
			input:    bson.D{{Key: "a", Value: int64(-9223372036854775808)}},
			expected: "{\"a\":-9223372036854775808}",
		},

		// Floating point types - normal values
		{
			desc:     "float32",
			input:    bson.D{{Key: "a", Value: float32(3.14)}},
			expected: "{\"a\":3.14}",
		},
		{
			desc:     "float64",
			input:    bson.D{{Key: "a", Value: float64(3.14159265359)}},
			expected: "{\"a\":3.14159265359}",
		},
		{
			desc:     "negative float32",
			input:    bson.D{{Key: "a", Value: float32(-3.14)}},
			expected: "{\"a\":-3.14}",
		},
		{
			desc:     "negative float64",
			input:    bson.D{{Key: "a", Value: float64(-3.14159265359)}},
			expected: "{\"a\":-3.14159265359}",
		},

		// Special float values - NaN and Infinity (these are handled by our custom codec)
		{
			desc:     "float64 NaN",
			input:    bson.D{{Key: "a", Value: math.NaN()}},
			expected: "{\"a\":\"NaN\"}",
		},
		{
			desc:     "float64 +Inf",
			input:    bson.D{{Key: "a", Value: math.Inf(1)}},
			expected: "{\"a\":\"+Inf\"}",
		},
		{
			desc:     "float64 -Inf",
			input:    bson.D{{Key: "a", Value: math.Inf(-1)}},
			expected: "{\"a\":\"-Inf\"}",
		},
		{
			desc:     "float32 NaN",
			input:    bson.D{{Key: "a", Value: float32(math.NaN())}},
			expected: "{\"a\":\"NaN\"}",
		},
		{
			desc:     "float32 +Inf",
			input:    bson.D{{Key: "a", Value: float32(math.Inf(1))}},
			expected: "{\"a\":\"+Inf\"}",
		},
		{
			desc:     "float32 -Inf",
			input:    bson.D{{Key: "a", Value: float32(math.Inf(-1))}},
			expected: "{\"a\":\"-Inf\"}",
		},

		// Byte and rune types
		{
			desc:     "byte",
			input:    bson.D{{Key: "a", Value: byte(65)}},
			expected: "{\"a\":65}",
		},
		{
			desc:     "rune",
			input:    bson.D{{Key: "a", Value: 'A'}},
			expected: "{\"a\":65}",
		},

		// Nil/null values
		{
			desc:     "nil",
			input:    bson.D{{Key: "a", Value: nil}},
			expected: "{\"a\":null}",
		},

		// Slice types
		{
			desc:     "[]int",
			input:    bson.D{{Key: "a", Value: []int{1, 2, 3}}},
			expected: "{\"a\":[1,2,3]}",
		},
		{
			desc:     "[]string",
			input:    bson.D{{Key: "a", Value: []string{"hello", "world"}}},
			expected: "{\"a\":[\"hello\",\"world\"]}",
		},
		{
			desc:     "empty slice",
			input:    bson.D{{Key: "a", Value: []int{}}},
			expected: "{\"a\":[]}",
		},

		// Array types
		{
			desc:     "[3]int",
			input:    bson.D{{Key: "a", Value: [3]int{1, 2, 3}}},
			expected: "{\"a\":[1,2,3]}",
		},
		{
			desc:     "[2]string",
			input:    bson.D{{Key: "a", Value: [2]string{"hello", "world"}}},
			expected: "{\"a\":[\"hello\",\"world\"]}",
		},

		// Map types
		{
			desc:     "map[string]int",
			input:    bson.D{{Key: "a", Value: map[string]int{"x": 1, "y": 2}}},
			expected: "{\"a\":{\"x\":1,\"y\":2}}",
		},
		{
			desc:     "map[string]string",
			input:    bson.D{{Key: "a", Value: map[string]string{"hello": "world"}}},
			expected: "{\"a\":{\"hello\":\"world\"}}",
		},
		{
			desc:     "empty map",
			input:    bson.D{{Key: "a", Value: map[string]int{}}},
			expected: "{\"a\":{}}",
		},

		//  type Date
		{
			desc:     "Date",
			input:    bson.D{{Key: "a", Value: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)}},
			expected: "{\"a\":\"2023-01-01T12:00:00Z\"}",
		},

		// Interface{} type
		{
			desc:     "interface{} with int",
			input:    bson.D{{Key: "a", Value: interface{}(42)}},
			expected: "{\"a\":42}",
		},
		{
			desc:     "interface{} with string",
			input:    bson.D{{Key: "a", Value: interface{}("hello")}},
			expected: "{\"a\":\"hello\"}",
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
			expected: "{\"str\":\"hello\",\"num\":42,\"bool\":true,\"null\":null}",
		},

		// Nested bson.D
		{
			desc: "nested bson.D",
			input: bson.D{{Key: "nested", Value: bson.D{
				{Key: "inner1", Value: "str"},
				{Key: "inner2", Value: 1},
				{Key: "inner3", Value: true},
				{
					Key: "inner3", Value: bson.D{
						{Key: "a", Value: math.NaN()},
						{Key: "b", Value: []string{"hello", "world"}},
					},
				},
			}}},
			expected: "{\"nested\":{\"inner1\":\"str\",\"inner2\":1,\"inner3\":true,\"inner3\":{\"a\":\"NaN\",\"b\":[\"hello\",\"world\"]}}}",
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
	require.Equal(t, "\"6893edbecb1f9508891bbb84\"", string(result))
}
