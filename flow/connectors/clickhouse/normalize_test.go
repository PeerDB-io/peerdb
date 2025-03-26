package connclickhouse

import (
	"testing"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func Test_GetOrderByColumns(t *testing.T) {
	tableMappingForTest := &protos.TableMapping{
		SourceTableIdentifier:      "test_table",
		DestinationTableIdentifier: "test_table_ch",
		Columns: []*protos.ColumnSetting{
			{
				SourceName:      "my id",
				DestinationName: "my id",
				Ordering:        1,
			},
			{
				SourceName:      "name",
				DestinationName: "name",
				Ordering:        2,
			},
		},
	}

	sourcePkeys := []string{"my id", "name"}
	colNameMap := map[string]string{
		"my id": "my id",
		"name":  "name",
	}

	expected := []string{"`my id`", "`name`"}
	actual := getOrderedOrderByColumns(tableMappingForTest, sourcePkeys, colNameMap)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}
}

func Test_GetOrderByColumns_NoOrdering(t *testing.T) {
	tableMappingForTest := &protos.TableMapping{
		SourceTableIdentifier:      "test_table",
		DestinationTableIdentifier: "test_table_ch",
		Columns: []*protos.ColumnSetting{
			{
				SourceName:      "my id",
				DestinationName: "my id",
			},
			{
				SourceName:      "name",
				DestinationName: "name",
			},
		},
	}

	sourcePkeys := []string{"my id"}
	colNameMap := map[string]string{
		"my id": "my id",
		"name":  "name",
	}

	expected := []string{"`my id`"}
	actual := getOrderedOrderByColumns(tableMappingForTest, sourcePkeys, colNameMap)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}
}
