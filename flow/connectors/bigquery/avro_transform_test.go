package connbigquery

import (
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestAvroTransform(t *testing.T) {
	dstSchema := &bigquery.Schema{
		&bigquery.FieldSchema{
			Name: "col1",
			Type: bigquery.GeographyFieldType,
		},
		&bigquery.FieldSchema{
			Name: "col2",
			Type: bigquery.JSONFieldType,
		},
		&bigquery.FieldSchema{
			Name: "camelCol4",
			Type: bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name: "sync_col",
			Type: bigquery.TimestampFieldType,
		},
	}

	expectedTransformCols := []string{
		"ST_GEOGFROMTEXT(`col1`) AS `col1`",
		"PARSE_JSON(`col2`,wide_number_mode=>'round') AS `col2`",
		"`camelCol4`",
		"CURRENT_TIMESTAMP AS `sync_col`",
	}
	transformedCols := getTransformedColumns(dstSchema, "sync_col", "del_col")
	if !reflect.DeepEqual(transformedCols, expectedTransformCols) {
		t.Errorf("Transform SQL is not correct. Got: %v", transformedCols)
	}
}
