package connbigquery

import (
	"reflect"
	"strings"
	"testing"
)

func TestGenerateUpdateStatement_WithUnchangedToastCols(t *testing.T) {
	m := &mergeStmtGenerator{}
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{"", "col2, col3", "col2", "col3"}

	expected := []string{
		"WHEN MATCHED AND (_peerdb_deduped._peerdb_record_type != 2)" +
			" AND _peerdb_unchanged_toast_columns='' " +
			"THEN UPDATE SET `col1` = _peerdb_deduped.col1," +
			" `col2` = _peerdb_deduped.col2," +
			" `col3` = _peerdb_deduped.col3",
		"WHEN MATCHED AND (_peerdb_deduped._peerdb_record_type != 2)" +
			" AND _peerdb_unchanged_toast_columns='col2, col3' " +
			"THEN UPDATE SET `col1` = _peerdb_deduped.col1",
		"WHEN MATCHED AND (_peerdb_deduped._peerdb_record_type != 2)" +
			" AND _peerdb_unchanged_toast_columns='col2'" +
			"THEN UPDATE SET `col1` = _peerdb_deduped.col1," +
			" `col3` = _peerdb_deduped.col3",
		"WHEN MATCHED AND (_peerdb_deduped._peerdb_record_type != 2)" +
			" AND _peerdb_unchanged_toast_columns='col3'" +
			"THEN UPDATE SET `col1` = _peerdb_deduped.col1," +
			" `col2` = _peerdb_deduped.col2",
	}

	result := m.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = removeSpacesTabsNewlines(expected[i])
		result[i] = removeSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestGenerateUpdateStatement_NoUnchangedToastCols(t *testing.T) {
	m := &mergeStmtGenerator{}
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{""}

	expected := []string{
		"WHEN MATCHED AND (_peerdb_deduped._peerdb_record_type != 2) " +
			"AND _peerdb_unchanged_toast_columns=''" +
			"THEN UPDATE SET " +
			"`col1` = _peerdb_deduped.col1," +
			" `col2` = _peerdb_deduped.col2," +
			" `col3` = _peerdb_deduped.col3",
	}

	result := m.generateUpdateStatements(allCols, unchangedToastCols)

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
