package parser

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/dop251/goja/ast"
	"github.com/dop251/goja/parser"
	"github.com/stretchr/testify/require"
)

// TestBSONLiterals tests BSON literal parsing with expected outputs.
//
// Note: Format validation (e.g., ISODate string format) is out of scope.
// Invalid formats are passed through to MongoDB for validation.
func TestBSONLiterals(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string // expected JSON output (empty for error cases)
		wantErr string // expected error substring (empty for success)
	}{
		// Literals (non-constructor)
		{"literal/null", "null", "null", ""},
		{"literal/true", "true", "true", ""},
		{"literal/false", "false", "false", ""},
		{"literal/int", "42", "42", ""},
		{"literal/negative_int", "-42", "-42", ""},
		{"literal/large_int", "9999999", "9999999", ""},
		{"literal/float", "3.14", "3.14", ""},
		{"literal/negative_float", "-3.14", "-3.14", ""},
		{"literal/string", `"hello"`, `"hello"`, ""},
		{"literal/array", "[1, 2, 3]", "[1,2,3]", ""},
		{"literal/object", "({a: 1, b: 2})", `{"a":1,"b":2}`, ""}, // parentheses needed for object literal
		{"literal/nested", "({a: {b: 1}})", `{"a":{"b":1}}`, ""},  // parentheses needed for object literal
		{"literal/regex", "/cat/i", `{"$regularExpression":{"pattern":"cat","options":"i"}}`, ""},

		// Special identifiers
		{"identifier/undefined", "undefined", `{"$undefined":true}`, ""},

		// MaxKey/MinKey
		{"MaxKey/without_new", "MaxKey()", `{"$maxKey":1}`, ""},
		{"MaxKey/with_new", "new MaxKey()", "", "unsupported constructor: new MaxKey()"},
		{"MaxKey/extra_args", "MaxKey(1)", "", "MaxKey() takes no arguments, got 1"},
		{"MinKey/without_new", "MinKey()", `{"$minKey":1}`, ""},
		{"MinKey/with_new", "new MinKey()", "", "unsupported constructor: new MinKey()"},
		{"MinKey/extra_args", "MinKey(1)", "", "MinKey() takes no arguments, got 1"},

		// ObjectId
		{"ObjectId/valid", "ObjectId('8f4a2b6c1d3e5f7890abcdef')", `{"$oid":"8f4a2b6c1d3e5f7890abcdef"}`, ""},
		{"ObjectId/with_new", "new ObjectId('8f4a2b6c1d3e5f7890abcdef')", "", "unsupported constructor: new ObjectId()"},
		{"ObjectId/0-arg", "ObjectId()", "", "ObjectId() requires exactly 1 argument, got 0"},

		// Timestamp
		{"Timestamp/valid", "Timestamp(5, 237)", `{"$timestamp":{"t":5,"i":237}}`, ""},
		{"Timestamp/with_new", "new Timestamp(0, 100)", "", "unsupported constructor: new Timestamp()"},
		{"Timestamp/0-arg", "Timestamp()", "", "Timestamp() requires exactly 2 arguments, got 0"},
		{"Timestamp/1-arg", "Timestamp(1)", "", "Timestamp() requires exactly 2 arguments, got 1"},

		// Date/ISODate
		{"ISODate/with_time_z", "ISODate('2023-04-18T15:42:33Z')", `{"$date":"2023-04-18T15:42:33Z"}`, ""},
		{"ISODate/with_offset", "ISODate('2023-04-18T15:42:33+02:00')", `{"$date":"2023-04-18T15:42:33+02:00"}`, ""},
		{"ISODate/with_ms", "ISODate('2023-04-18T15:42:33.789Z')", `{"$date":"2023-04-18T15:42:33.789Z"}`, ""},
		{"ISODate/epoch_0", "ISODate(0)", `{"$date":{"$numberLong":"0"}}`, ""},
		{"ISODate/epoch_null", "ISODate(null)", `{"$date":{"$numberLong":"0"}}`, ""},
		{"ISODate/epoch_ms", "ISODate(8765)", `{"$date":{"$numberLong":"8765"}}`, ""},
		{"ISODate/with_new", "new ISODate()", `{"$date":"now"}`, ""},
		{"ISODate/0-arg", "ISODate()", "", "ISODate() requires exactly 1 argument, got 0"},
		{"ISODate/date_only", "ISODate('2023-04-18')", "", "requires full ISO 8601 format"},
		{"ISODate/invalid_format", "ISODate('bah')", "", "requires full ISO 8601 format"},
		{"Date/valid", "Date('2023-04-18T15:42:33Z')", `{"$date":"2023-04-18T15:42:33Z"}`, ""},
		{"Date/date_only", "Date('2023-04-18')", "", "requires full ISO 8601 format"},
		{"Date/object_arg", "Date({})", "", "Date() argument must be a string or number"},
		{"Date/0-arg", "Date()", "", "Date() requires exactly 1 argument, got 0"},
		{"new_Date/no_args", "new Date()", `{"$date":"now"}`, ""},
		{"new_Date/string", "new Date('2023-04-18T15:42:33Z')", `{"$date":"2023-04-18T15:42:33Z"}`, ""},
		{"new_Date/epoch", "new Date(9876543210)", `{"$date":{"$numberLong":"9876543210"}}`, ""},
		{"new_Date/date_only", "new Date('2023-04-18')", "", "requires full ISO 8601 format"},
		{"new_ISODate/no_args", "new ISODate()", `{"$date":"now"}`, ""},
		{"new_ISODate/string", "new ISODate('2023-04-18T15:42:33Z')", `{"$date":"2023-04-18T15:42:33Z"}`, ""},
		{"new_ISODate/date_only", "new ISODate('2023-04-18')", "", "requires full ISO 8601 format"},

		// BinData
		{"BinData/valid", `BinData(128, "dGVzdGluZw==")`, `{"$binary":{"base64":"dGVzdGluZw==","subType":"80"}}`, ""},
		{"BinData/subtype_0", `BinData(0, "dGVzdGluZw==")`, `{"$binary":{"base64":"dGVzdGluZw==","subType":"00"}}`, ""},
		{"BinData/0-arg", "BinData()", "", "BinData() requires exactly 2 arguments, got 0"},
		{"BinData/1-arg", "BinData(0)", "", "BinData() requires exactly 2 arguments, got 1"},

		// UUID
		{"UUID/with_dashes", "UUID('f1e2d3c4-b5a6-9780-1234-56789abcdef0')", `{"$uuid":"f1e2d3c4-b5a6-9780-1234-56789abcdef0"}`, ""},
		{"UUID/without_dashes", "UUID('abcdef0123456789abcdef0123456789')", `{"$uuid":"abcdef0123456789abcdef0123456789"}`, ""},
		{"UUID/0-arg", "UUID()", "", "UUID() requires exactly 1 argument, got 0"},

		// Numeric: NumberInt/Int32
		{"NumberInt/string", "NumberInt('7')", `{"$numberInt":"7"}`, ""},
		{"NumberInt/number", "NumberInt(456)", `{"$numberInt":"456"}`, ""},
		{"NumberInt/float", "NumberInt(789.3)", `{"$numberInt":"789"}`, ""},
		{"NumberInt/0-arg", "NumberInt()", "", "NumberInt() requires exactly 1 argument, got 0"},
		{"Int32/string", "Int32('91')", `{"$numberInt":"91"}`, ""},
		{"Int32/number", "Int32(91)", `{"$numberInt":"91"}`, ""},
		{"Int32/0-arg", "Int32()", "", "Int32() requires exactly 1 argument, got 0"},

		// Numeric: NumberLong/Long
		{"NumberLong/string", "NumberLong('456')", `{"$numberLong":"456"}`, ""},
		{"NumberLong/large_string", "NumberLong('123456789012345678')", `{"$numberLong":"123456789012345678"}`, ""},
		{"NumberLong/number", "NumberLong(54321098765)", `{"$numberLong":"54321098765"}`, ""},
		{"NumberLong/float", "NumberLong(456.7)", `{"$numberLong":"456"}`, ""},
		{"NumberLong/0-arg", "NumberLong()", "", "NumberLong() requires exactly 1 argument, got 0"},
		{"Long/string", "Long('777')", `{"$numberLong":"777"}`, ""},
		{"Long/number", "Long(777)", `{"$numberLong":"777"}`, ""},
		{"Long/0-arg", "Long()", "", "Long() requires exactly 1 argument, got 0"},

		// Numeric: NumberDecimal/Decimal128
		{"NumberDecimal/string", "NumberDecimal('456.7')", `{"$numberDecimal":"456.7"}`, ""},
		{"NumberDecimal/large_string", "NumberDecimal('987654321098765432.1')", `{"$numberDecimal":"987654321098765432.1"}`, ""},
		{"NumberDecimal/number", "NumberDecimal(456)", `{"$numberDecimal":"456"}`, ""},
		{"NumberDecimal/0-arg", "NumberDecimal()", "", "NumberDecimal() requires exactly 1 argument, got 0"},
		{"Decimal128/string", "Decimal128('789.012')", `{"$numberDecimal":"789.012"}`, ""},
		{"Decimal128/number", "Decimal128(456)", `{"$numberDecimal":"456"}`, ""},
		{"Decimal128/float", "Decimal128(789.012)", `{"$numberDecimal":"789.012"}`, ""},
		{"Decimal128/0-arg", "Decimal128()", "", "Decimal128() requires exactly 1 argument, got 0"},

		// Numeric: Double/NumberDouble
		{"Double/string", "Double('6.28')", `{"$numberDouble":"6.28"}`, ""},
		{"Double/number", "Double(6.28)", `{"$numberDouble":"6.28"}`, ""},
		{"Double/int", "Double(73)", `{"$numberDouble":"73"}`, ""},
		{"Double/0-arg", "Double()", "", "Double() requires exactly 1 argument, got 0"},
		{"NumberDouble/string", "NumberDouble('1.41')", `{"$numberDouble":"1.41"}`, ""},
		{"NumberDouble/0-arg", "NumberDouble()", "", "NumberDouble() requires exactly 1 argument, got 0"},

		// new RegExp() variations
		{"new_RegExp/pattern_only", "new RegExp('cat')", `{"$regularExpression":{"pattern":"cat","options":""}}`, ""},
		{"new_RegExp/with_flags", "new RegExp('cat', 'gi')", `{"$regularExpression":{"pattern":"cat","options":"gi"}}`, ""},
		{"new_RegExp/no_args", "new RegExp()", "", "new RegExp() requires at least 1 argument"},

		// Unsupported constructors
		{"DBRef/unsupported", "DBRef('namespace', 'oid')", "", "unsupported constructor: DBRef"},
		{"BSONSymbol/unsupported", "BSONSymbol('test')", "", "unsupported constructor: BSONSymbol"},
		{"Code/unsupported", "Code('code')", "", "unsupported constructor: Code"},
		{"HexData/unsupported", "HexData(0, 'abc')", "", "unsupported constructor: HexData"},
		{"MD5/unsupported", "MD5('abc')", "", "unsupported constructor: MD5"},
		{"bsonsize/unsupported", "bsonsize({})", "", "unsupported constructor: bsonsize"},
		{"unknown/FooBar", "FooBar()", "", "unsupported constructor: FooBar"},
		{"unknown/RandomThing", "RandomThing(1, 2, 3)", "", "unsupported constructor: RandomThing"},
	}

	update := os.Getenv("UPDATE_GOLDEN") != ""

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prog, err := parser.ParseFile(nil, "", tt.input, 0)
			if err != nil {
				if tt.wantErr == "" {
					require.NoError(t, err, "parse error")
				}
				return // Parse error counts as expected error
			}

			require.Len(t, prog.Body, 1, "expected 1 statement")

			exprStmt, ok := prog.Body[0].(*ast.ExpressionStatement)
			require.True(t, ok, "expected expression statement")

			got, err := evalLiteral(exprStmt.Expression)

			if tt.wantErr != "" {
				require.Error(t, err, "expected error, got success")
				require.Contains(t, err.Error(), tt.wantErr, "error message mismatch")
				return
			}

			require.NoError(t, err, "evalLiteral failed")

			gotJSON, jsonErr := json.Marshal(got)
			require.NoError(t, jsonErr)

			if update {
				t.Logf("UPDATE: {%q, %q, `%s`, %q},", tt.name, tt.input, string(gotJSON), tt.wantErr)
			}

			require.JSONEq(t, tt.want, string(gotJSON))
		})
	}
}
