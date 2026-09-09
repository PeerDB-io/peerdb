package connpostgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
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
		{
			name:     "two relaxed integers in array",
			input:    `{"value": [` + relaxedNumberStr + `, 1, ` + negRelaxedNumberStr + `]}`,
			expected: []any{relaxedNumberStr, float64(1), negRelaxedNumberStr},
		},
		{
			name:     "two relaxed integers in nested object",
			input:    `{"value": {"a": ` + relaxedNumberStr + `, "b": {"c": ` + negRelaxedNumberStr + `}}}`,
			expected: map[string]any{"a": relaxedNumberStr, "b": map[string]any{"c": negRelaxedNumberStr}},
		},
		{
			name:     "adjacent relaxed integers in array",
			input:    `{"value": [` + relaxedNumberStr + `,` + negRelaxedNumberStr + `]}`,
			expected: []any{relaxedNumberStr, negRelaxedNumberStr},
		},
	}

	jsonApi := createExtendedJSONUnmarshaler()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result map[string]any
			var err error
			if useJsonMarshaller {
				err = jsonApi.UnmarshalFromString(tc.input, &result)
			} else {
				transformed, err2 := convertWithRelaxedNumbers(bytes.NewBufferString(tc.input), len(tc.input))
				require.NoError(t, err2)
				require.True(t, json.Valid(transformed), "output is not valid JSON: %q", transformed)
				err = json.Unmarshal(transformed, &result)
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

func TestConvertRelaxedNumberInvalidUTF8(t *testing.T) {
	relaxedNumberStr := "1" + strings.Repeat("0", 1000)

	testCases := []struct {
		name     string
		input    string
		expected any
	}{
		{
			name:     "invalid utf8 in string value",
			input:    "{\"value\": \"bad\xffutf8\"}",
			expected: "bad�utf8",
		},
		{
			name:     "truncated multibyte sequence",
			input:    "{\"value\": \"bad\xe2\x82utf8\"}",
			expected: "bad��utf8",
		},
		{
			name:     "lone surrogate escape",
			input:    `{"value": "bad\ud800utf8"}`,
			expected: "bad�utf8",
		},
		{
			name:     "invalid utf8 and relaxed number",
			input:    "{\"value\": \"bad\xffutf8\", \"num\": " + relaxedNumberStr + "}",
			expected: "bad�utf8",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			transformed, err := convertWithRelaxedNumbers(bytes.NewBufferString(tc.input), len(tc.input))
			require.NoError(t, err)
			require.True(t, json.Valid(transformed), "output is not valid JSON: %q", transformed)
			require.True(t, utf8.Valid(transformed), "output is not valid UTF-8: %q", transformed)

			var result map[string]any
			require.NoError(t, json.Unmarshal(transformed, &result))
			require.Equal(t, tc.expected, result["value"])
			if _, ok := result["num"]; ok {
				require.Equal(t, relaxedNumberStr, result["num"])
			}
		})
	}
}

func TestConvertRelaxedNumberMultiple(t *testing.T) {
	big := "1" + strings.Repeat("0", 400)
	negBig := "-" + big

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "two out of range floats",
			input:    `{"a": ` + big + `, "b": ` + negBig + `}`,
			expected: `{"a": "` + big + `", "b": "` + negBig + `"}`,
		},
		{
			name:     "two adjacent out of range floats",
			input:    `[` + big + `,` + negBig + `]`,
			expected: `["` + big + `","` + negBig + `"]`,
		},
		{
			name:     "out of range floats interleaved with valid values",
			input:    `{"a": ` + big + `, "b": 1.5, "c": ` + negBig + `, "d": "x", "e": [` + big + `, 2]}`,
			expected: `{"a": "` + big + `", "b": 1.5, "c": "` + negBig + `", "d": "x", "e": ["` + big + `", 2]}`,
		},
		{
			name:     "three out of range floats at varying depths",
			input:    `{"a": {"b": ` + big + `}, "c": [[` + negBig + `], {"d": ` + big + `}], "e": 7}`,
			expected: `{"a": {"b": "` + big + `"}, "c": [["` + negBig + `"], {"d": "` + big + `"}], "e": 7}`,
		},
		{
			// Only overflow is reported as out of range; an underflowing exponent
			// parses as 0 without error, so it is left as a number.
			name:     "out of range floats with exponents",
			input:    `{"a": 1e400, "b": -2.5E+400, "c": 1e-400}`,
			expected: `{"a": "1e400", "b": "-2.5E+400", "c": 1e-400}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			transformed, err := convertWithRelaxedNumbers(bytes.NewBufferString(tc.input), len(tc.input))
			require.NoError(t, err)
			require.True(t, json.Valid(transformed), "output is not valid JSON: %q", transformed)
			require.Equal(t, tc.expected, string(transformed))
		})
	}
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

func benchmarkJsonProcessing(b *testing.B, fastPath bool, numFields, maxDepth int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42)) //nolint:gosec
	doc := constructDocument(rng, numFields, maxDepth)
	marshaledDoc, err := json.Marshal(doc)
	require.NoError(b, err)
	jsonIter := createExtendedJSONUnmarshaler()
	var result any

	b.ResetTimer()

	for b.Loop() {
		if fastPath {
			_, err := convertWithRelaxedNumbers(bytes.NewBuffer(marshaledDoc), len(marshaledDoc))
			require.NoError(b, err)
		} else {
			require.NoError(b, jsonIter.UnmarshalFromString(string(marshaledDoc), &result))
			_, err := json.Marshal(result)
			require.NoError(b, err)
		}
	}
}

func benchmarkJsonProcessingCases(b *testing.B, useFastPath bool) {
	b.Helper()
	tcs := []struct {
		numFields, maxDepth int
	}{{4, 32}, {8, 4}, {8, 8}, {8, 16}, {64, 2}, {64, 4}}
	for _, tc := range tcs {
		b.Run(fmt.Sprintf("numFields=%d/maxDepth=%d", tc.numFields, tc.maxDepth), func(b *testing.B) {
			benchmarkJsonProcessing(b, useFastPath, tc.numFields, tc.maxDepth)
		})
	}
}

func BenchmarkConvertRelaxedNumber(b *testing.B) {
	benchmarkJsonProcessingCases(b, true)
}

func BenchmarkRelaxedNumberExtension(b *testing.B) {
	benchmarkJsonProcessingCases(b, false)
}
