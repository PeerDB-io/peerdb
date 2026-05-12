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
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestMarshalDocument(t *testing.T) {
	oid, _ := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")

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
		{
			desc:     "String with backslash",
			input:    bson.D{{Key: "a", Value: `back\slash`}},
			expected: `{"a":"back\\slash"}`,
		},
		{
			desc:     "String with control chars",
			input:    bson.D{{Key: "a", Value: "\x00\x01\x02\x1f"}},
			expected: `{"a":"\u0000\u0001\u0002\u001f"}`,
		},
		{
			desc:     "String with HTML entities",
			input:    bson.D{{Key: "a", Value: "a&b<c>d"}},
			expected: `{"a":"a\u0026b\u003cc\u003ed"}`,
		},
		{
			desc:     "String with HTML script",
			input:    bson.D{{Key: "a", Value: "<script>alert('xss')</script>"}},
			expected: `{"a":"\u003cscript\u003ealert('xss')\u003c/script\u003e"}`,
		},
		{
			desc:     "String with unicode line separators",
			input:    bson.D{{Key: "a", Value: "line\u2028sep\u2029par"}},
			expected: `{"a":"line\u2028sep\u2029par"}`,
		},
		{
			desc:     "String with unicode multibyte",
			input:    bson.D{{Key: "a", Value: "日本語テスト"}},
			expected: `{"a":"日本語テスト"}`,
		},
		{
			desc:     "String with emoji",
			input:    bson.D{{Key: "a", Value: "hello 🌍🚀"}},
			expected: `{"a":"hello 🌍🚀"}`,
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
			desc:     "int32 zero",
			input:    bson.D{{Key: "a", Value: int32(0)}},
			expected: `{"a":0}`,
		},
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
			desc:     "int64 zero",
			input:    bson.D{{Key: "a", Value: int64(0)}},
			expected: `{"a":0}`,
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
			desc:     "float64 zero",
			input:    bson.D{{Key: "a", Value: float64(0)}},
			expected: `{"a":0.0}`,
		},
		{
			desc:     "float64 negative zero",
			input:    bson.D{{Key: "a", Value: math.Copysign(0, -1)}},
			expected: `{"a":-0.0}`,
		},
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
			desc:     "float64 integer valued",
			input:    bson.D{{Key: "a", Value: float64(3)}},
			expected: `{"a":3.0}`,
		},
		{
			desc:     "float64 very small",
			input:    bson.D{{Key: "a", Value: 1e-7}},
			expected: `{"a":1e-07}`,
		},
		{
			desc:     "float64 very large",
			input:    bson.D{{Key: "a", Value: 1e21}},
			expected: `{"a":1e+21}`,
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

		// Special float values
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
			desc:     "bson.DateTime epoch",
			input:    bson.D{{Key: "date", Value: bson.DateTime(0)}},
			expected: `{"date":"1970-01-01T00:00:00Z"}`,
		},
		{
			desc:     "bson.DateTime pre-1970",
			input:    bson.D{{Key: "date", Value: bson.DateTime(-1)}},
			expected: `{"date":"1969-12-31T23:59:59.999Z"}`,
		},
		{
			desc:     "bson.DateTime",
			input:    bson.D{{Key: "date", Value: bson.DateTime(1672531200000)}}, // 2023-01-01 00:00:00 UTC
			expected: `{"date":"2023-01-01T00:00:00Z"}`,
		},
		{
			desc:     "bson.DateTime with millis",
			input:    bson.D{{Key: "date", Value: bson.DateTime(1672531200123)}},
			expected: `{"date":"2023-01-01T00:00:00.123Z"}`,
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
			desc:     "bson.DateTime min",
			input:    bson.D{{Key: "date", Value: bson.DateTime(math.MinInt64)}},
			expected: `{"date":"-292275055-05-16T16:47:04.192Z"}`,
		},

		// Null values
		{
			desc:     "nil",
			input:    bson.D{{Key: "a", Value: nil}},
			expected: `{"a":null}`,
		},
		{
			desc:     "bson.Null",
			input:    bson.D{{Key: "a", Value: bson.Null{}}},
			expected: `{"a":null}`,
		},

		// ObjectID
		{
			desc:     "bson.ObjectID",
			input:    bson.D{{Key: "id", Value: oid}},
			expected: `{"id":"507f1f77bcf86cd799439011"}`,
		},

		// JavaScript
		{
			desc:     "bson.JavaScript",
			input:    bson.D{{Key: "js", Value: bson.JavaScript("function() { return 42; }")}},
			expected: `{"js":"function() { return 42; }"}`,
		},

		// Symbol
		{
			desc:     "bson.Symbol",
			input:    bson.D{{Key: "symbol", Value: bson.Symbol("test_symbol")}},
			expected: `{"symbol":"test_symbol"}`,
		},

		// Binary
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
			desc:  "bson.Binary UUID",
			input: bson.D{{Key: "binary", Value: bson.Binary{Subtype: 0x04, Data: []byte("0123456789abcdef")}}},
			expected: fmt.Sprintf(`{"binary":{"Subtype":4,"Data":"%s"}}`,
				base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))),
		},

		// Timestamp
		{
			desc:     "bson.Timestamp",
			input:    bson.D{{Key: "ts", Value: bson.Timestamp{T: 1672531200, I: 1}}},
			expected: `{"ts":{"T":1672531200,"I":1}}`,
		},
		{
			desc:     "bson.Timestamp zero",
			input:    bson.D{{Key: "ts", Value: bson.Timestamp{T: 0, I: 0}}},
			expected: `{"ts":{"T":0,"I":0}}`,
		},

		// Regex
		{
			desc:     "bson.Regex",
			input:    bson.D{{Key: "regex", Value: bson.Regex{Pattern: "^test.*", Options: "i"}}},
			expected: `{"regex":{"Pattern":"^test.*","Options":"i"}}`,
		},
		{
			desc:     "bson.Regex empty options",
			input:    bson.D{{Key: "regex", Value: bson.Regex{Pattern: ".*", Options: ""}}},
			expected: `{"regex":{"Pattern":".*","Options":""}}`,
		},

		// Decimal128
		{
			desc:     "bson.Decimal128",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "123.4567890987654321")}},
			expected: `{"d":"123.4567890987654321"}`,
		},
		{
			desc:     "bson.Decimal128 negative",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "-123.4567890987654321")}},
			expected: `{"d":"-123.4567890987654321"}`,
		},
		{
			desc:     "bson.Decimal128 zero",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "0")}},
			expected: `{"d":"0"}`,
		},
		{
			desc:     "bson.Decimal128 negative zero",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "-0")}},
			expected: `{"d":"-0"}`,
		},
		{
			desc:     "bson.Decimal128 integer valued",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "100")}},
			expected: `{"d":"100"}`,
		},
		{
			desc:     "bson.Decimal128 negative integer valued",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "-100")}},
			expected: `{"d":"-100"}`,
		},
		{
			desc:     "bson.Decimal128 very small",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "1E-6176")}},
			expected: `{"d":"1E-6176"}`,
		},
		{
			desc:     "bson.Decimal128 negative very small",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "-1E-6176")}},
			expected: `{"d":"-1E-6176"}`,
		},
		{
			desc:     "bson.Decimal128 max",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "9.999999999999999999999999999999999E+6144")}},
			expected: `{"d":"9.999999999999999999999999999999999E+6144"}`,
		},
		{
			desc:     "bson.Decimal128 negative max",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "-9.999999999999999999999999999999999E+6144")}},
			expected: `{"d":"-9.999999999999999999999999999999999E+6144"}`,
		},
		{
			desc:     "bson.Decimal128 NaN",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "NaN")}},
			expected: `{"d":"NaN"}`,
		},
		{
			desc:     "bson.Decimal128 Infinity",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "Infinity")}},
			expected: `{"d":"Infinity"}`,
		},
		{
			desc:     "bson.Decimal128 -Infinity",
			input:    bson.D{{Key: "d", Value: mustParseDec128(t, "-Infinity")}},
			expected: `{"d":"-Infinity"}`,
		},

		// Undefined
		{
			desc:     "bson.Undefined",
			input:    bson.D{{Key: "undefined", Value: bson.Undefined{}}},
			expected: `{"undefined":{}}`,
		},

		// MinKey / MaxKey
		{
			desc:     "bson.MinKey",
			input:    bson.D{{Key: "min", Value: bson.MinKey{}}},
			expected: `{"min":{}}`,
		},
		{
			desc:     "bson.MaxKey",
			input:    bson.D{{Key: "max", Value: bson.MaxKey{}}},
			expected: `{"max":{}}`,
		},

		// DBPointer (deprecated)
		{
			desc:     "bson.DBPointer",
			input:    bson.D{{Key: "dbptr", Value: bson.DBPointer{DB: "test_db", Pointer: oid}}},
			expected: `{"dbptr":{"DB":"test_db","Pointer":"507f1f77bcf86cd799439011"}}`,
		},

		// CodeWithScope (deprecated)
		{
			desc: "bson.CodeWithScope",
			input: bson.D{{Key: "cws", Value: bson.CodeWithScope{
				Code:  "function(x) { return x + y; }",
				Scope: bson.D{{Key: "y", Value: int32(10)}},
			}}},
			expected: `{"cws":{"Code":"function(x) { return x + y; }","Scope":{"y":10}}}`,
		},
		{
			desc: "bson.CodeWithScope empty scope",
			input: bson.D{{Key: "cws", Value: bson.CodeWithScope{
				Code:  "function() { return 1 + 2; }",
				Scope: bson.D{},
			}}},
			expected: `{"cws":{"Code":"function() { return 1 + 2; }","Scope":{}}}`,
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

		// Nested documents
		{
			desc:     "nested doc empty",
			input:    bson.D{{Key: "d", Value: bson.D{}}},
			expected: `{"d":{}}`,
		},
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
		{
			desc: "nested doc deep",
			input: bson.D{{Key: "a", Value: bson.D{
				{Key: "b", Value: bson.D{{Key: "c", Value: bson.D{{Key: "d", Value: int32(42)}}}}},
			}}},
			expected: `{"a":{"b":{"c":{"d":42}}}}`,
		},

		// Arrays
		{
			desc:     "array empty",
			input:    bson.D{{Key: "a", Value: bson.A{}}},
			expected: `{"a":[]}`,
		},
		{
			desc:     "bson.A",
			input:    bson.D{{Key: "a", Value: bson.A{1, "str", true}}},
			expected: `{"a":[1,"str",true]}`,
		},
		{
			desc:     "bson.A mixed types",
			input:    bson.D{{Key: "a", Value: bson.A{int32(1), "two", true, nil, 3.14}}},
			expected: `{"a":[1,"two",true,null,3.14]}`,
		},
		{
			desc:     "bson.A special values",
			input:    bson.D{{Key: "a", Value: bson.A{math.NaN(), math.Inf(1), math.Inf(-1), bson.DateTime(math.MaxInt64)}}},
			expected: `{"a":["NaN","+Inf","-Inf","292278994-08-17T07:12:55.807Z"]}`,
		},
		{
			desc: "array nested",
			input: bson.D{{Key: "a", Value: bson.A{
				bson.A{int32(1), int32(2)}, bson.A{int32(3), int32(4)},
			}}},
			expected: `{"a":[[1,2],[3,4]]}`,
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

		// Nested special values
		{
			desc:     "nested NaN in doc",
			input:    bson.D{{Key: "d", Value: bson.D{{Key: "x", Value: math.NaN()}}}},
			expected: `{"d":{"x":"NaN"}}`,
		},
		{
			desc:     "nested Inf in array",
			input:    bson.D{{Key: "a", Value: bson.A{math.Inf(1), math.Inf(-1)}}},
			expected: `{"a":["+Inf","-Inf"]}`,
		},
		{
			desc:     "nested DateTime in array",
			input:    bson.D{{Key: "a", Value: bson.A{bson.DateTime(1672531200000)}}},
			expected: `{"a":["2023-01-01T00:00:00Z"]}`,
		},
		{
			desc: "nested specials deep",
			input: bson.D{{Key: "d", Value: bson.D{
				{Key: "arr", Value: bson.A{
					bson.D{{Key: "v", Value: math.NaN()}},
					bson.D{{Key: "v", Value: bson.DateTime(0)}},
					bson.D{{Key: "v", Value: math.Inf(-1)}},
				}},
			}}},
			expected: `{"d":{"arr":[{"v":"NaN"},{"v":"1970-01-01T00:00:00Z"},{"v":"-Inf"}]}}`,
		},

		// Realistic multi-field document
		{
			desc: "realistic document",
			input: bson.D{
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
				{Key: "score", Value: mustParseDec128(t, "123.4567890987654321")},
				{Key: "notes", Value: nil},
			},
			expected: `{"_id":"507f1f77bcf86cd799439011","name":"John Doe","email":"john@example.com","age":30,` +
				`"balance":1234.56,"active":true,"created":"2023-01-01T00:00:00Z","tags":["admin","user"],` +
				`"address":{"street":"123 Main St","city":"Springfield","zip":12345},` +
				`"score":"123.4567890987654321","notes":null}`,
		},
	}

	converter := NewDirectBsonConverter()
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			inputRaw, err := bson.Marshal(test.input)
			require.NoError(t, err)

			result, err := converter.QValueJSONFromDocument(inputRaw)
			require.NoError(t, err)
			require.Equal(t, test.expected, result.Val)
		})
	}
}

func mustParseDec128(t *testing.T, s string) bson.Decimal128 {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	require.NoError(t, err)
	return d
}

func TestMarshalId(t *testing.T) {
	toIdRaw := func(val any) bson.RawValue {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: val}})
		require.NoError(t, err)
		return bson.Raw(raw).Lookup("_id")
	}
	converter := NewDirectBsonConverter()

	objectId, err := bson.ObjectIDFromHex("6893edbecb1f9508891bbb84")
	require.NoError(t, err)
	objectIdRaw := toIdRaw(objectId)

	// Test new version behavior (no redundant quotes for ObjectID and string)
	qValue, err := converter.QValueStringFromId(objectIdRaw, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, `6893edbecb1f9508891bbb84`, qValue.Val)

	stringIdRaw := toIdRaw("string_pk")
	qValue, err = converter.QValueStringFromId(stringIdRaw, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, `string_pk`, qValue.Val)

	intIdRaw := toIdRaw(123)
	qValue, err = converter.QValueStringFromId(intIdRaw, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, `123`, qValue.Val)

	compositeIdRaw := toIdRaw(bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 2}})
	qValue, err = converter.QValueStringFromId(compositeIdRaw, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, `{"a":1,"b":2}`, qValue.Val)

	// Test old version behavior (JSON quoting preserved for backwards compatibility)
	qValue, err = converter.QValueStringFromId(objectIdRaw, shared.InternalVersion_First)
	require.NoError(t, err)
	require.Equal(t, `"6893edbecb1f9508891bbb84"`, qValue.Val)

	qValue, err = converter.QValueStringFromId(stringIdRaw, shared.InternalVersion_First)
	require.NoError(t, err)
	require.Equal(t, `"string_pk"`, qValue.Val)
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

func TestQValuesFromBsonRawInvalidIds(t *testing.T) {
	converter := NewDirectBsonConverter()

	t.Run("null _id is rejected", func(t *testing.T) {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: nil}})
		require.NoError(t, err)
		_, err = QValuesFromBsonRaw(raw, shared.InternalVersion_Latest, converter, "test_table")
		require.ErrorAs(t, err, new(*exceptions.MongoInvalidIdValueError))
	})

	t.Run("missing _id is rejected", func(t *testing.T) {
		raw, err := bson.Marshal(bson.D{{Key: "not_id", Value: "value"}})
		require.NoError(t, err)
		_, err = QValuesFromBsonRaw(raw, shared.InternalVersion_Latest, converter, "test_table")
		require.ErrorAs(t, err, new(*exceptions.MongoInvalidIdValueError))
	})
}
