package connsnowflake

import (
	"strings"
	"testing"
)

func TestAvroTransform(t *testing.T) {
	colNames := []string{"col1", "col2", "col3", "camelCol4", "sync_col", "del_col"}
	colTypes := []string{"GEOGRAPHY", "VARIANT", "NUMBER", "STRING", "TIMESTAMP_LTZ", "BOOLEAN"}

	expectedTransform := `TO_GEOGRAPHY($1:"col1"::string, true) AS "COL1",` +
		`PARSE_JSON($1:"col2") AS "COL2",` +
		`$1:"col3" AS "COL3",` +
		`($1:"camelCol4")::STRING AS "camelCol4",` +
		`CURRENT_TIMESTAMP AS "SYNC_COL",` +
		`FALSE AS "DEL_COL"`
	transform, cols := getTransformSQL(colNames, colTypes, "sync_col", "del_col", nil)
	if transform != expectedTransform {
		t.Errorf("Transform SQL is not correct. Got: %v", transform)
	}

	expectedCols := `"COL1","COL2","COL3","camelCol4","SYNC_COL","DEL_COL"`
	if cols != expectedCols {
		t.Errorf("Columns are not correct. Got:%v", cols)
	}
}

// The destination can store a column with different case than the source name, e.g. with
// QUOTED_IDENTIFIERS_IGNORE_CASE "partProjectId" is created as PARTPROJECTID, but the Avro field keeps its case.
func TestAvroTransformMatchesAvroFieldCase(t *testing.T) {
	// as returned by SHOW COLUMNS, destination stores uppercase
	colNames := []string{"PARTPROJECTID", "PART_PROJECT_ID", "ID", "OTHER", "SYNC_COL", "DEL_COL"}
	colTypes := []string{"STRING", "STRING", "NUMBER", "STRING", "TIMESTAMP_LTZ", "BOOLEAN"}
	avroFields := []string{"partProjectId", "part_project_id", "ID", "other"}

	avroFieldByCol, err := matchAvroFieldNames(colNames[:4], avroFields)
	if err != nil {
		t.Fatal(err)
	}

	expectedTransform := `($1:"partProjectId")::STRING AS "PARTPROJECTID",` +
		`($1:"part_project_id")::STRING AS "PART_PROJECT_ID",` +
		`$1:"ID" AS "ID",` +
		`($1:"other")::STRING AS "OTHER",` +
		`CURRENT_TIMESTAMP AS "SYNC_COL",` +
		`FALSE AS "DEL_COL"`
	transform, cols := getTransformSQL(colNames, colTypes, "SYNC_COL", "DEL_COL", avroFieldByCol)
	if transform != expectedTransform {
		t.Errorf("Transform SQL is not correct. Got: %v", transform)
	}
	expectedCols := `"PARTPROJECTID","PART_PROJECT_ID","ID","OTHER","SYNC_COL","DEL_COL"`
	if cols != expectedCols {
		t.Errorf("Columns are not correct. Got:%v", cols)
	}
}

func TestAvroTransformUnmatchedColumnKeepsOldLookup(t *testing.T) {
	// a column without an Avro field (or with no field names read) is looked up as before
	avroFieldByCol, err := matchAvroFieldNames([]string{"MISSING", "camelMissing"}, []string{"other"})
	if err != nil {
		t.Fatal(err)
	}
	if len(avroFieldByCol) != 0 {
		t.Fatalf("expected no matches, got %v", avroFieldByCol)
	}

	expected := `($1:"missing")::STRING AS "MISSING",($1:"camelMissing")::STRING AS "camelMissing"`
	for _, byCol := range []map[string]string{nil, avroFieldByCol} {
		transform, _ := getTransformSQL([]string{"MISSING", "camelMissing"}, []string{"STRING", "STRING"}, "", "", byCol)
		if transform != expected {
			t.Errorf("Transform SQL is not correct. Got: %v", transform)
		}
	}
}

func TestMatchAvroFieldNames(t *testing.T) {
	tests := []struct {
		name       string
		colNames   []string
		avroFields []string
		expected   map[string]string
	}{
		{
			name:       "camelCase stored uppercase",
			colNames:   []string{"PARTPROJECTID"},
			avroFields: []string{"partProjectId"},
			expected:   map[string]string{"PARTPROJECTID": "partProjectId"},
		},
		{
			name:       "snake_case stored uppercase",
			colNames:   []string{"PART_PROJECT_ID"},
			avroFields: []string{"part_project_id"},
			expected:   map[string]string{"PART_PROJECT_ID": "part_project_id"},
		},
		{
			name:       "lowercase source, uppercase destination",
			colNames:   []string{"ID"},
			avroFields: []string{"id"},
			expected:   map[string]string{"ID": "id"},
		},
		{
			name:       "all-caps source",
			colNames:   []string{"ID"},
			avroFields: []string{"ID"},
			expected:   map[string]string{"ID": "ID"},
		},
		{
			name:       "mixed case stored as is",
			colNames:   []string{"camelCol4"},
			avroFields: []string{"camelCol4"},
			expected:   map[string]string{"camelCol4": "camelCol4"},
		},
		{
			name:       "exact match wins over case-insensitive match",
			colNames:   []string{"ID"},
			avroFields: []string{"id", "ID"},
			expected:   map[string]string{"ID": "ID"},
		},
		{
			name:       "no matching field",
			colNames:   []string{"MISSING"},
			avroFields: []string{"other"},
			expected:   map[string]string{},
		},
		{
			name:       "no field names",
			colNames:   []string{"ID"},
			avroFields: nil,
			expected:   map[string]string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchAvroFieldNames(tc.colNames, tc.avroFields)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.expected) {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
			for col, field := range tc.expected {
				if got[col] != field {
					t.Errorf("column %s: expected field %q, got %q", col, field, got[col])
				}
			}
		})
	}
}

func TestMatchAvroFieldNamesAmbiguous(t *testing.T) {
	_, err := matchAvroFieldNames([]string{"PARTPROJECTID"}, []string{"partProjectId", "partprojectid"})
	if err == nil {
		t.Fatal("expected an error for two Avro fields matching one column")
	}
	for _, want := range []string{"PARTPROJECTID", "partProjectId", "partprojectid"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}
