package connpostgres

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func testRelaxedNumber(t *testing.T, useJsonMarshaller bool) {
	t.Helper()
	relaxedNumberStr := "1" + strings.Repeat("0", 1000)
	negRelaxedNumberStr := "-" + relaxedNumberStr

	testCases := []struct {
		name     string
		input    string
		expected any
	}{
		{
			name:     "integer",
			input:    `{"value": 42}`,
			expected: float64(42),
		},
		{
			name:     "float",
			input:    `{"value": 3.14159}`,
			expected: float64(3.14159),
		},
		{
			name:     "large integer",
			input:    `{"value": 1` + strings.Repeat("0", 308) + `}`,
			expected: float64(1e308),
		},
		{
			name:     "negative large integer",
			input:    `{"value": -1` + strings.Repeat("0", 308) + `}`,
			expected: float64(-1e308),
		},
		{
			name:     "scientific notation",
			input:    `{"value": 1.23e10}`,
			expected: float64(1.23e10),
		},
		{
			name:     "relaxed integer",
			input:    `{"value": ` + relaxedNumberStr + `}`,
			expected: relaxedNumberStr,
		},
		{
			name:     "negative relaxed integer",
			input:    `{"value": ` + negRelaxedNumberStr + `}`,
			expected: negRelaxedNumberStr,
		},
		{
			name:     "string",
			input:    `{"value": "not a number"}`,
			expected: "not a number",
		},
		{
			name:     "boolean",
			input:    `{"value": true}`,
			expected: true,
		},
		{
			name:     "null",
			input:    `{"value": null}`,
			expected: nil,
		},
		{
			name:     "array with numbers",
			input:    `{"value": [1, 2.5, ` + relaxedNumberStr + `]}`,
			expected: []any{float64(1), float64(2.5), relaxedNumberStr},
		},
		{
			name:     "nested object with numbers",
			input:    `{"value": {"a": 123, "b": ` + relaxedNumberStr + `}}`,
			expected: map[string]any{"a": float64(123), "b": relaxedNumberStr},
		},
	}

	jsonApi, _ := createExtendedJSONUnmarshaler()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result map[string]any
			var err error
			if useJsonMarshaller {
				err = jsonApi.UnmarshalFromString(tc.input, &result)
			} else {
				transformed, err2 := convertWithRelaxedNumbers(tc.input)
				require.NoError(t, err2)
				err = json.Unmarshal([]byte(transformed), &result)
			}
			require.NoError(t, err)

			actual := result["value"]

			// For arrays and maps, need to compare deeply
			switch expected := tc.expected.(type) {
			case []any:
				actualArr, ok := actual.([]any)
				require.True(t, ok, "expected array type")
				require.Equal(t, expected, actualArr)
			case map[string]any:
				actualMap, ok := actual.(map[string]any)
				require.True(t, ok, "expected map type")
				require.Equal(t, expected, actualMap)
			default:
				require.Equal(t, tc.expected, actual)
			}
		})
	}
}

func TestRelaxedNumberExtension(t *testing.T) {
	testRelaxedNumber(t, true)
}

func TestConvertRelaxedNumber(t *testing.T) {
	testRelaxedNumber(t, false)
}

func TestConvertRelaxedNumberPreservesRawStrings(t *testing.T) {
	big := "1" + strings.Repeat("0", 1000)
	input := " \n{\t\"\\u006b\" : \"\\u0061\", \"duplicate\":1, \"duplicate\":2, \"big\":" + big + "} "
	want := " \n{\t\"\\u006b\" : \"\\u0061\", \"duplicate\":1, \"duplicate\":2, \"big\":\"" + big + "\"} "

	got, err := convertWithRelaxedNumbers(input)
	require.NoError(t, err)
	require.Equal(t, want, string(got))
}

func TestConvertRelaxedNumberRepairsInvalidUnicode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "invalid UTF-8", input: "{\"value\":\"\xff\"}", want: "{\"value\":\"�\"}"},
		{name: "lone surrogate", input: `{"value":"\uD800"}`, want: `{"value":"�"}`},
		{name: "duplicate keys", input: "{\"x\":1,\"x\":\"\xff\"}", want: "{\"x\":1,\"x\":\"�\"}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertWithRelaxedNumbers(tc.input)
			require.NoError(t, err)
			require.Equal(t, preMarshalledJson(tc.want), got)
		})
	}
}

func TestCopyWithRelaxedNumbers(t *testing.T) {
	overflowInteger := "1" + strings.Repeat("0", 1000)
	inRange308Digits := strings.Repeat("9", 308)
	inRange309Digits := "1" + strings.Repeat("0", 308)
	overflow309Digits := "2" + strings.Repeat("0", 308)

	for _, tc := range []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "preserves every in-range token byte",
			input: " \n{\t\"integer\" : 9007199254740993, \"fraction\":1.2300, \"exponent\":1e+02," +
				` "escaped":"\u0061", "duplicate":1, "duplicate":2 } `,
			want: " \n{\t\"integer\" : 9007199254740993, \"fraction\":1.2300, \"exponent\":1e+02," +
				` "escaped":"\u0061", "duplicate":1, "duplicate":2 } `,
		},
		{name: "quotes positive overflow", input: `{"value": 1e309}`, want: `{"value": "1e309"}`},
		{name: "quotes negative overflow", input: `[-1.8e308,-` + overflowInteger + `]`, want: `["-1.8e308","-` + overflowInteger + `"]`},
		{name: "quotes nested overflow only", input: `{"array":[1,2.50,{"huge":` + overflowInteger + `}],"text":"1e999"}`, want: `{"array":[1,2.50,{"huge":"` + overflowInteger + `"}],"text":"1e999"}`},
		{name: "maximum float64 remains a number", input: `1.7976931348623157e308`, want: `1.7976931348623157e308`},
		{name: "308 integer digits remain a number", input: inRange308Digits, want: inRange308Digits},
		{name: "309 digit integer inside range remains a number", input: inRange309Digits, want: inRange309Digits},
		{name: "309 digit integer outside range is quoted", input: overflow309Digits, want: `"` + overflow309Digits + `"`},
		{name: "long fraction remains a number", input: `0.` + strings.Repeat("9", 1000), want: `0.` + strings.Repeat("9", 1000)},
		{name: "underflow remains a number", input: `{"minimum":5e-324,"underflow":1e-324}`, want: `{"minimum":5e-324,"underflow":1e-324}`},
		{name: "number-like string is ignored", input: `{"value":"escaped \\\" 1e999 and slash \\\\ end","number":1e309}`, want: `{"value":"escaped \\\" 1e999 and slash \\\\ end","number":"1e309"}`},
		{name: "top-level null", input: `null`, want: `null`},
		{name: "top-level string", input: `"unchanged"`, want: `"unchanged"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := copyWithRelaxedNumbers(tc.input)
			require.NoError(t, err)
			require.Equal(t, preMarshalledJson(tc.want), got)

			gotAgain, err := copyWithRelaxedNumbers(string(got))
			require.NoError(t, err)
			require.Equal(t, got, gotAgain, "transformation must be idempotent")
		})
	}
}

func TestConvertWithRelaxedNumbersRejectsInvalidJSON(t *testing.T) {
	for _, input := range []string{"", " ", `{"incomplete":`, `{"leadingZero":01}`, `{} {}`, `null true`, `[] trailing`} {
		t.Run(input, func(t *testing.T) {
			_, err := convertWithRelaxedNumbers(input)
			require.Error(t, err)
		})
	}
}

func TestDecodeColumnDataPreMarshalledJSON(t *testing.T) {
	typeMap := pgtype.NewMap()
	source := &PostgresCDCSource{
		PostgresConnector: &PostgresConnector{
			typeMap:     typeMap,
			hushWarnOID: make(map[uint32]struct{}),
		},
		internalVersion:        shared.InternalVersion_Latest,
		fastProcessJsonColumns: true,
	}
	overflowInteger := "1" + strings.Repeat("0", 1000)

	for _, tc := range []struct {
		name       string
		oid        uint32
		formatCode int16
		input      []byte
		want       string
	}{
		{
			name:       "json text",
			oid:        pgtype.JSONOID,
			formatCode: pgtype.TextFormatCode,
			input:      []byte(` {"precise":9007199254740993,"huge":` + overflowInteger + `} `),
			want:       ` {"precise":9007199254740993,"huge":"` + overflowInteger + `"} `,
		},
		{
			name:       "jsonb binary",
			oid:        pgtype.JSONBOID,
			formatCode: pgtype.BinaryFormatCode,
			input:      append([]byte{1}, []byte(`{"huge":1e309}`)...),
			want:       `{"huge":"1e309"}`,
		},
		{
			name:       "json null is not SQL null",
			oid:        pgtype.JSONOID,
			formatCode: pgtype.TextFormatCode,
			input:      []byte(`null`),
			want:       `null`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := source.decodeColumnData(
				tc.input,
				tc.oid,
				-1,
				tc.formatCode,
				nil,
				shared.InternalVersion_Latest,
			)
			require.NoError(t, err)
			jsonValue, ok := got.(types.QValueJSON)
			require.True(t, ok)
			require.Equal(t, tc.want, jsonValue.Val)
		})
	}

	got, err := source.decodeColumnData(
		nil,
		pgtype.JSONOID,
		-1,
		pgtype.TextFormatCode,
		nil,
		shared.InternalVersion_Latest,
	)
	require.NoError(t, err)
	require.Nil(t, got.Value(), "SQL null must remain distinct from JSON null")

	arrayInput := pgtype.FlatArray[pgtype.Text]{
		{String: `{"exact":1.2300}`, Valid: true},
		{},
		{String: `1e309`, Valid: true},
	}
	encodedArray, err := typeMap.Encode(pgtype.JSONArrayOID, pgtype.TextFormatCode, arrayInput, nil)
	require.NoError(t, err)
	got, err = source.decodeColumnData(
		encodedArray,
		pgtype.JSONArrayOID,
		-1,
		pgtype.TextFormatCode,
		nil,
		shared.InternalVersion_Latest,
	)
	require.NoError(t, err)
	jsonArray, ok := got.(types.QValueJSON)
	require.True(t, ok)
	require.True(t, jsonArray.IsArray)
	require.Equal(t, `[{"exact":1.2300},null,"1e309"]`, jsonArray.Val)
}

func FuzzConvertWithRelaxedNumbers(f *testing.F) {
	for _, seed := range []string{
		`null`,
		`{"a":[1,2.50,1e309]}`,
		`{"duplicate":1,"duplicate":2}`,
		`["escaped\nstring",true,false]`,
		`{"lone-surrogate":"\uD800"}`,
		"{\"invalid-utf8\":\"\xff\"}",
		`invalid`,
	} {
		f.Add(seed)
	}

	permissiveOptions := []jsontext.Options{
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
	}
	strictOptions := []jsontext.Options{
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(false),
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 1<<20 {
			t.Skip()
		}

		permissiveValid := jsontext.Value(input).IsValid(permissiveOptions...)
		got, err := convertWithRelaxedNumbers(input)
		if !permissiveValid {
			require.Error(t, err)
			return
		}
		require.NoError(t, err)
		require.True(t, jsontext.Value(got).IsValid(strictOptions...))

		dec := jsontext.NewDecoder(strings.NewReader(string(got)), strictOptions...)
		for {
			tok, err := dec.ReadToken()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			if tok.Kind() == jsontext.KindNumber {
				_, err = tok.Float()
				require.NoError(t, err, "output must not contain an out-of-range number")
			}
		}

		gotAgain, err := convertWithRelaxedNumbers(string(got))
		require.NoError(t, err)
		require.Equal(t, got, gotAgain, "transformation must be idempotent")
	})
}

func TestDuplicateJsonKeysCounter(t *testing.T) {
	t.Parallel()
	jsonApi, ext := createExtendedJSONUnmarshaler()

	testCases := []struct {
		name  string
		input string
		want  int64
	}{
		{name: "no duplicates", input: `{"a":1,"b":2}`, want: 0},
		{name: "top level duplicate", input: `{"a":1,"a":2}`, want: 1},
		{name: "nested duplicates", input: `{"o":{"k":1,"k":2,"k":3}}`, want: 2},
		{name: "duplicate inside array element", input: `[{"x":1,"x":2}]`, want: 1},
		{name: "duplicate keys with distinct values elsewhere", input: `{"a":{"b":1},"a":{"b":2},"c":3}`, want: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before := ext.duplicateKeys.Load()
			var result any
			require.NoError(t, jsonApi.UnmarshalFromString(tc.input, &result))
			require.Equal(t, tc.want, ext.duplicateKeys.Load()-before)
		})
	}

	// last occurrence wins, matching encoding/json
	var result any
	require.NoError(t, jsonApi.UnmarshalFromString(`{"a":1,"a":2}`, &result))
	require.InEpsilon(t, float64(2), result.(map[string]any)["a"], 0.00001)
}

func generateString(rng *rand.Rand, length int) string {
	const randStringCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var sb strings.Builder
	sb.Grow(length) // Optimize memory allocation

	for range length {
		sb.WriteByte(randStringCharset[rng.Intn(len(randStringCharset))])
	}
	return sb.String()
}

func constructDocument(rng *rand.Rand, numFields int, maxDepth int) map[string]any {
	const keyLength = 32

	result := make(map[string]any, numFields)
	for range numFields {
		f := rng.Float32()
		// 33% chance each of producing a random number, random string, or another object.
		// If we've reached maxDepth, the object part folds into a random number.
		if f < 0.33 && maxDepth > 0 {
			result[generateString(rng, keyLength)] = constructDocument(rng, numFields, maxDepth-1)
		} else if f < 0.66 {
			result[generateString(rng, keyLength)] = rng.ExpFloat64()
		} else {
			result[generateString(rng, keyLength)] = generateString(rng, 64)
		}
	}
	return result
}

type jsonProcessingBenchmarkMode int

const (
	benchmarkRawCopyWithRepair jsonProcessingBenchmarkMode = iota
	benchmarkRawCopyPreserveInvalidUnicode
	benchmarkNormalizeStrings
	benchmarkUnmarshalMarshal
)

var (
	benchmarkConvertedJSON preMarshalledJson
	benchmarkMarshaledJSON []byte
)

func benchmarkJsonProcessing(b *testing.B, mode jsonProcessingBenchmarkMode, numFields, maxDepth int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42)) //nolint:gosec
	doc := constructDocument(rng, numFields, maxDepth)
	marshaledDoc, err := json.Marshal(doc)
	require.NoError(b, err)
	benchmarkJsonProcessingInput(b, mode, marshaledDoc)
}

func benchmarkJsonProcessingInput(b *testing.B, mode jsonProcessingBenchmarkMode, marshaledDoc []byte) {
	b.Helper()
	jsonIter, _ := createExtendedJSONUnmarshaler()
	input := string(marshaledDoc)
	var result any
	b.SetBytes(int64(len(marshaledDoc)))

	b.ResetTimer()

	for b.Loop() {
		switch mode {
		case benchmarkRawCopyWithRepair:
			var err error
			benchmarkConvertedJSON, err = convertWithRelaxedNumbers(input)
			require.NoError(b, err)
		case benchmarkRawCopyPreserveInvalidUnicode:
			var err error
			benchmarkConvertedJSON, err = copyWithRelaxedNumbersUnicode(input, true)
			require.NoError(b, err)
		case benchmarkNormalizeStrings:
			var err error
			benchmarkConvertedJSON, err = reencodeWithRelaxedNumbers(input)
			require.NoError(b, err)
		case benchmarkUnmarshalMarshal:
			require.NoError(b, jsonIter.UnmarshalFromString(input, &result))
			var err error
			benchmarkMarshaledJSON, err = json.Marshal(result)
			require.NoError(b, err)
		default:
			b.Fatalf("unknown benchmark mode: %d", mode)
		}
	}
	b.ReportMetric(float64(len(marshaledDoc)), "B/input")
}

func benchmarkInjectObjectField(input, field []byte) ([]byte, error) {
	decoder := jsontext.NewDecoder(bytes.NewReader(input), jsontext.AllowDuplicateNames(true))
	midpoint := int64(len(input) / 2)
	for {
		token, err := decoder.ReadToken()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("no object ending after byte %d", midpoint)
			}
			return nil, err
		}
		if token.Kind() != '}' || decoder.InputOffset() < midpoint {
			continue
		}

		insertAt := int(decoder.InputOffset()) - 1
		injected := make([]byte, 0, len(input)+len(field))
		injected = append(injected, input[:insertAt]...)
		injected = append(injected, field...)
		injected = append(injected, input[insertAt:]...)
		if json.Valid(injected) {
			return injected, nil
		}
	}
}

func benchmarkInjectLongNumber(input []byte, number string) ([]byte, error) {
	return benchmarkInjectObjectField(input, []byte(`,"long_number":`+number))
}

func benchmarkInjectInvalidUnicode(input []byte) ([]byte, error) {
	return benchmarkInjectObjectField(input, []byte(
		",\"invalid_utf8\":\"\xff\",\"lone_surrogate\":\"\\uD800\"",
	))
}

func benchmarkExternalJSONCases(b *testing.B, overflowInteger string) []struct {
	name  string
	input []byte
} {
	b.Helper()

	paths := []string{
		filepath.Join("testdata", "github-json", "issues.json"),
		filepath.Join("testdata", "github-json", "commits.json"),
		filepath.Join("testdata", "github-json", "events.json"),
	}
	paths = append(paths, filepath.SplitList(os.Getenv("PEERDB_JSON_BENCH_FILES"))...)

	var cases []struct {
		name  string
		input []byte
	}
	for _, path := range paths {
		if path == "" {
			continue
		}
		input, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		if !json.Valid(input) {
			b.Fatalf("%s is not valid JSON", path)
		}
		prepared, err := convertWithRelaxedNumbers(string(input))
		if err != nil {
			b.Fatalf("preparing %s: %v", path, err)
		}
		if string(prepared) != string(input) {
			b.Fatalf("preparing %s changed an input with no overflowing number", path)
		}

		name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		cases = append(cases, struct {
			name  string
			input []byte
		}{name: name + "-no-substitution", input: input})

		injected, err := benchmarkInjectLongNumber(input, overflowInteger)
		if err != nil {
			b.Fatalf("injecting long number into %s: %v", path, err)
		}
		prepared, err = convertWithRelaxedNumbers(string(injected))
		if err != nil {
			b.Fatalf("preparing injected %s: %v", path, err)
		}
		expected := bytes.Replace(injected, []byte(overflowInteger), []byte(`"`+overflowInteger+`"`), 1)
		if string(prepared) != string(expected) {
			b.Fatalf("preparing injected %s changed tokens other than the long number", path)
		}
		cases = append(cases, struct {
			name  string
			input []byte
		}{name: name + "-middle-long-number", input: injected})

		invalidUnicode, err := benchmarkInjectInvalidUnicode(input)
		if err != nil {
			b.Fatalf("injecting invalid Unicode into %s: %v", path, err)
		}
		repaired, err := convertWithRelaxedNumbers(string(invalidUnicode))
		if err != nil {
			b.Fatalf("repairing invalid Unicode in %s: %v", path, err)
		}
		if !jsontext.Value(repaired).IsValid(
			jsontext.AllowDuplicateNames(true),
			jsontext.AllowInvalidUTF8(false),
		) {
			b.Fatalf("repairing invalid Unicode in %s did not produce strict JSON", path)
		}
		cases = append(cases, struct {
			name  string
			input []byte
		}{name: name + "-invalid-unicode", input: invalidUnicode})
	}
	return cases
}

func benchmarkJsonProcessingModes(b *testing.B, input []byte) {
	b.Helper()
	modes := []struct {
		name string
		mode jsonProcessingBenchmarkMode
	}{
		{name: "legacy-object-decode-encode", mode: benchmarkUnmarshalMarshal},
		{name: "json-token-decode-encode", mode: benchmarkNormalizeStrings},
		{name: "raw-copy-preserve-invalid-unicode", mode: benchmarkRawCopyPreserveInvalidUnicode},
		{name: "raw-copy-with-unicode-repair", mode: benchmarkRawCopyWithRepair},
	}
	for _, mode := range modes {
		b.Run(mode.name, func(b *testing.B) {
			benchmarkJsonProcessingInput(b, mode.mode, input)
		})
	}
}

func benchmarkJsonProcessingCases(b *testing.B, mode jsonProcessingBenchmarkMode) {
	b.Helper()
	tcs := []struct {
		numFields, maxDepth int
	}{{4, 32}, {8, 4}, {8, 8}, {8, 16}, {64, 2}, {64, 4}}
	for _, tc := range tcs {
		b.Run(fmt.Sprintf("numFields=%d/maxDepth=%d", tc.numFields, tc.maxDepth), func(b *testing.B) {
			benchmarkJsonProcessing(b, mode, tc.numFields, tc.maxDepth)
		})
	}
}

func BenchmarkConvertRelaxedNumberRawCopyWithRepair(b *testing.B) {
	benchmarkJsonProcessingCases(b, benchmarkRawCopyWithRepair)
}

func BenchmarkConvertRelaxedNumberRawCopyPreserveInvalidUnicode(b *testing.B) {
	benchmarkJsonProcessingCases(b, benchmarkRawCopyPreserveInvalidUnicode)
}

func BenchmarkConvertRelaxedNumberNormalizeStrings(b *testing.B) {
	benchmarkJsonProcessingCases(b, benchmarkNormalizeStrings)
}

func BenchmarkRelaxedNumberExtension(b *testing.B) {
	benchmarkJsonProcessingCases(b, benchmarkUnmarshalMarshal)
}

func BenchmarkPostgresJSONPreparation(b *testing.B) {
	overflowInteger := "32239232323" + strings.Repeat("239232323", 110)
	largeString := strings.Repeat("abcdefgh", 128*1024)
	cases := []struct {
		name  string
		input []byte
	}{
		{name: "small-no-substitution", input: []byte(`{"id":9007199254740993,"ratio":1.2300,"name":"example"}`)},
		{name: "small-one-substitution", input: []byte(`{"id":1,"huge":` + overflowInteger + `}`)},
		{name: "1MiB-no-substitution", input: []byte(`{"payload":"` + largeString + `","count":42}`)},
		{name: "1MiB-one-substitution", input: []byte(`{"payload":"` + largeString + `","huge":` + overflowInteger + `}`)},
	}
	cases = append(cases, benchmarkExternalJSONCases(b, overflowInteger)...)

	for _, test := range cases {
		b.Run(test.name, func(b *testing.B) {
			benchmarkJsonProcessingModes(b, test.input)
		})
	}
}

func BenchmarkJsonStringHandling(b *testing.B) {
	const targetSize = 1 << 20
	escapedString := strings.Repeat(`\u0061`, targetSize/len(`\u0061`))
	invalidUTF8 := []byte(`{"value":"` + strings.Repeat("abcdefgh", targetSize/(2*len("abcdefgh"))))
	invalidUTF8 = append(invalidUTF8, 0xff)
	invalidUTF8 = append(invalidUTF8, strings.Repeat("abcdefgh", targetSize/(2*len("abcdefgh")))...)
	invalidUTF8 = append(invalidUTF8, []byte(`"}`)...)
	unicodeHalf := strings.Repeat("abcdefgh", targetSize/(2*len("abcdefgh")))
	loneSurrogate := []byte(`{"value":"` + unicodeHalf + `\uD800` + unicodeHalf + `"}`)

	inputs := []struct {
		name  string
		input []byte
	}{
		{name: "escaped-unicode", input: []byte(`{"value":"` + escapedString + `"}`)},
		{name: "invalid-utf8", input: invalidUTF8},
		{name: "lone-surrogate", input: loneSurrogate},
	}

	for _, input := range inputs {
		b.Run(input.name, func(b *testing.B) {
			benchmarkJsonProcessingModes(b, input.input)
		})
	}
}
