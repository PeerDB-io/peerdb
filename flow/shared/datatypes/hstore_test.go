package datatypes

import (
	"testing"
)

func TestHStoreHappy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		input  string
		output string
	}{
		{
			"Happy",
			`"a"=>"b", "c"=>"d"`,
			`{"a":"b","c":"d"}`,
		},
		{
			"EscapedQuotes",
			`"a\"b"=>"c\"d"`,
			`{"a\"b":"c\"d"}`,
		},
		{
			"EscapedBackslashes",
			`"a\\b"=>"c\\d"`,
			`{"a\\b":"c\\d"}`,
		},
		{
			"NullCase",
			`"a"=>NULL`,
			`{"a":null}`,
		},
		{
			"DisguisedSeparator",
			`"=>"=>"a=>b"`,
			`{"=\u003e":"a=\u003eb"}`,
		},
		{
			"EmptyToSpace",
			`""=>" "`,
			`{"":" "}`,
		},
		{
			"EmptyToEmpty",
			`""=>""`,
			`{"":""}`,
		},
		{
			"Duplicate",
			`"a"=>"b", "a"=>"c"`,
			`{"a":"c"}`,
		},
	} {
		result, err := ParseHstore(tc.input)
		if err != nil {
			t.Errorf("[%s] Unexpected error: %v", tc.name, err)
		}

		if result != tc.output {
			t.Errorf("[%s] Unexpected result. Expected: %v, but got: %v", tc.name, tc.output, result)
		}
	}
}

func TestInvalidInput(t *testing.T) {
	for _, input := range []string{
		`a=>NULL`,
		`"a"`,
		`"a"NULL`,
		`"a"=>NULL, `,
		`"a"=>NULL,`,
		`"a"=>NULLq`,
		`"a"=>NUL`,
		`"a"=>NO`,
		`"a"=>N`,
		`"a"=>O`,
		`"a"=>"a`,
		`"a"=>`,
		`"a`,
		`"`,
		`"a\"`,
		`"a\"\`,
		`"a\a"`,
	} {
		if _, err := ParseHstore(input); err == nil {
			t.Errorf("Unexpected success for %s", input)
		}
	}
}
