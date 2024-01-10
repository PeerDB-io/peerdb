package hstore_util

import (
	"testing"
)

func TestHStoreHappy(t *testing.T) {
	testCase := `"a"=>"b", "c"=>"d"`
	expected := `{"a":"b","c":"d"}`

	result, err := ParseHstore(testCase)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestHStoreEscapedQuotes(t *testing.T) {
	testCase := `"a\"b"=>"c\"d"`
	expected := `{"a\"b":"c\"d"}`

	result, err := ParseHstore(testCase)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestHStoreEscapedBackslashes(t *testing.T) {
	testCase := `"a\\b"=>"c\\d"`
	expected := `{"a\\b":"c\\d"}`

	result, err := ParseHstore(testCase)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestHStoreNullCase(t *testing.T) {
	testCase := `"a"=>NULL`
	expected := `{"a":null}`

	result, err := ParseHstore(testCase)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestHStoreDisguisedSeparator(t *testing.T) {
	testCase := `"=>"=>"a=>b"`
	expected := `{"=\u003e":"a=\u003eb"}`

	result, err := ParseHstore(testCase)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestHStoreEmpty(t *testing.T) {
	testCase := `""=>" "`
	expected := `{"":" "}`

	result, err := ParseHstore(testCase)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestHStoreDuplicate(t *testing.T) {
	testCase := `"a"=>"b", "a"=>"c"`
	expected := `{"a":"c"}`

	result, err := ParseHstore(testCase)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}
