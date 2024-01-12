package connsnowflake

import "testing"

func TestAvroTransform(t *testing.T) {
	colNames := []string{"col1", "col2", "col3", "camelCol4", "sync_col"}
	colTypes := []string{"GEOGRAPHY", "VARIANT", "NUMBER", "STRING", "TIMESTAMP_LTZ"}

	expectedTransform := `TO_GEOGRAPHY($1:"col1"::string, true) AS "COL1",` +
		`PARSE_JSON($1:"col2") AS "COL2",` +
		`$1:"col3" AS "COL3",` +
		`($1:"camelCol4")::STRING AS "camelCol4",` +
		`CURRENT_TIMESTAMP AS "SYNC_COL"`
	transform, cols := GetTransformSQL(colNames, colTypes, "sync_col")
	if transform != expectedTransform {
		t.Errorf("Transform SQL is not correct. Got: %v", transform)
	}

	expectedCols := `"COL1","COL2","COL3","camelCol4","SYNC_COL"`
	if cols != expectedCols {
		t.Errorf("Columns are not correct. Got:%v", cols)
	}
}
