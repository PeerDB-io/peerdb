package connsnowflake

import (
	"reflect"
	"strings"
	"testing"
)

func TestGenerateUpdateStatement_WithUnchangedToastCols(t *testing.T) {
	c := &SnowflakeConnector{}
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{"", "col2,col3", "col2", "col3"}

	expected := []string{
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col2,col3'
		THEN UPDATE SET "COL1" = SOURCE."COL1",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col2'
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col3'
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
	}
	result := c.generateUpdateStatements("_PEERDB_SYNCED_AT", "_PEERDB_SOFT_DELETE", false, allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = removeSpacesTabsNewlines(expected[i])
		result[i] = removeSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestGenerateUpdateStatement_EmptyColumns(t *testing.T) {
	c := &SnowflakeConnector{}
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{""}

	expected := []string{
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
	}
	result := c.generateUpdateStatements("_PEERDB_SYNCED_AT", "_PEERDB_SOFT_DELETE", false, allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = removeSpacesTabsNewlines(expected[i])
		result[i] = removeSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func removeSpacesTabsNewlines(s string) string {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\t", "")
	s = strings.ReplaceAll(s, "\n", "")
	return s
}
