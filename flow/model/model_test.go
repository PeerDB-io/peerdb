package model

import (
	"testing"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func TestToJSONWithOptsColKeySame(t *testing.T) {
	records := NewRecordItemWithData([]string{"col1", "col2", "col3"}, []qvalue.QValue{
		{Value: "val1", Kind: qvalue.QValueKindString},
		{Value: "val2", Kind: qvalue.QValueKindString},
		{Value: `{"key1": "val1", "col3": "something"}`, Kind: qvalue.QValueKindJSON},
	})

	jsonOpts := NewToJSONOptions([]string{"col3"})

	jsonRes, err := records.ToJSONWithOpts(&ToJSONOptions{
		UnnestColumns: jsonOpts.UnnestColumns,
	})

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := `{"col1":"val1","col2":"val2","col3":"something","key1":"val1"}`
	if jsonRes != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, jsonRes)
	}
}

func TestToJSONWithOptsRegular(t *testing.T) {
	records := NewRecordItemWithData([]string{"col1", "col2", "col3"}, []qvalue.QValue{
		{Value: "val1", Kind: qvalue.QValueKindString},
		{Value: "val2", Kind: qvalue.QValueKindString},
		{Value: `{"key1": "val1"}`, Kind: qvalue.QValueKindJSON},
	})

	jsonOpts := NewToJSONOptions([]string{"col3"})

	jsonRes, err := records.ToJSONWithOpts(&ToJSONOptions{
		UnnestColumns: jsonOpts.UnnestColumns,
	})

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := `{"col1":"val1","col2":"val2","key1":"val1"}`
	if jsonRes != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, jsonRes)
	}
}
