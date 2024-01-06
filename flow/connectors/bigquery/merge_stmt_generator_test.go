package connbigquery

import (
	"reflect"
	"strings"
	"testing"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func TestGenerateUpdateStatement_WithUnchangedToastCols(t *testing.T) {
	m := &mergeStmtGenerator{}
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{"", "col2, col3", "col2", "col3"}

	expected := []string{
		"WHEN MATCHED AND _rt!=2 AND _ut=''" +
			" THEN UPDATE SET `col1`=_d.col1,`col2`=_d.col2,`col3`=_d.col3," +
			"`synced_at`=CURRENT_TIMESTAMP,`deleted`=FALSE",
		"WHEN MATCHED AND _rt=2 " +
			"AND _ut='' " +
			"THEN UPDATE SET `col1`=_d.col1,`col2`=_d.col2," +
			"`col3`=_d.col3,`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE",
		"WHEN MATCHED AND _rt!=2 AND _ut='col2,col3' " +
			"THEN UPDATE SET `col1`=_d.col1,`synced_at`=CURRENT_TIMESTAMP,`deleted`=FALSE ",
		"WHEN MATCHED AND _rt=2 AND _ut='col2,col3' " +
			"THEN UPDATE SET `col1`=_d.col1,`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE",
		"WHEN MATCHED AND _rt!=2 " +
			"AND _ut='col2' " +
			"THEN UPDATE SET `col1`=_d.col1,`col3`=_d.col3," +
			"`synced_at`=CURRENT_TIMESTAMP,`deleted`=FALSE",
		"WHEN MATCHED AND _rt=2 " +
			"AND _ut='col2' " +
			"THEN UPDATE SET `col1`=_d.col1,`col3`=_d.col3," +
			"`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE ",
		"WHEN MATCHED AND _rt!=2 AND _ut='col3' " +
			"THEN UPDATE SET `col1`=_d.col1," +
			"`col2`=_d.col2,`synced_at`=CURRENT_TIMESTAMP,`deleted`=FALSE ",
		"WHEN MATCHED AND _rt=2 AND _ut='col3' " +
			"THEN UPDATE SET `col1`=_d.col1," +
			"`col2`=_d.col2,`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE",
	}

	result := m.generateUpdateStatements(allCols, unchangedToastCols, &protos.PeerDBColumns{
		SoftDelete:        true,
		SoftDeleteColName: "deleted",
		SyncedAtColName:   "synced_at",
	})

	for i := range expected {
		expected[i] = removeSpacesTabsNewlines(expected[i])
		result[i] = removeSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v,\nbut got: %v", expected, result)
	}
}

func TestGenerateUpdateStatement_NoUnchangedToastCols(t *testing.T) {
	m := &mergeStmtGenerator{}
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{""}

	expected := []string{
		"WHEN MATCHED AND _rt!=2 " +
			"AND _ut=''" +
			"THEN UPDATE SET " +
			"`col1`=_d.col1," +
			"`col2`=_d.col2," +
			"`col3`=_d.col3," +
			"`synced_at`=CURRENT_TIMESTAMP," +
			"`deleted`=FALSE",
		"WHEN MATCHED AND" +
			"_rt=2 AND _ut=''" +
			"THEN UPDATE SET `col1`=_d.col1,`col2`=_d.col2, " +
			"`col3`=_d.col3,`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE",
	}

	result := m.generateUpdateStatements(allCols, unchangedToastCols,
		&protos.PeerDBColumns{
			SoftDelete:        true,
			SoftDeleteColName: "deleted",
			SyncedAtColName:   "synced_at",
		})

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
