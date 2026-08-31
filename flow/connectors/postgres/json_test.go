package connpostgres

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testRelaxedNumber(t *testing.T, useJsonMarshaller bool) {
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
				transformed, err2 := convertWithRelaxedNumbers(strings.NewReader(tc.input), len(tc.input))
				require.NoError(t, err2)
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
	require.Equal(t, float64(2), result.(map[string]any)["a"])
}
