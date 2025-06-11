package connbigquery

import (
	"reflect"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestGenerateUpdateStatement(t *testing.T) {
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{""}
	m := &mergeStmtGenerator{
		shortColumn: map[string]string{
			"col1": "_c0",
			"col2": "_c1",
			"col3": "_c2",
		},
		peerdbCols: &protos.PeerDBColumns{
			SoftDeleteColName: "",
			SyncedAtColName:   "synced_at",
		},
	}

	expected := []string{
		"WHEN MATCHED AND _rt!=2 " +
			"AND _ut=''" +
			"THEN UPDATE SET " +
			"`col1`=_d._c0," +
			"`col2`=_d._c1," +
			"`col3`=_d._c2," +
			"`synced_at`=CURRENT_TIMESTAMP",
	}

	result := m.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestGenerateUpdateStatement_WithSoftDelete(t *testing.T) {
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{""}
	m := &mergeStmtGenerator{
		shortColumn: map[string]string{
			"col1": "_c0",
			"col2": "_c1",
			"col3": "_c2",
		},
		peerdbCols: &protos.PeerDBColumns{
			SoftDeleteColName: "deleted",
			SyncedAtColName:   "synced_at",
		},
	}

	expected := []string{
		"WHEN MATCHED AND _rt!=2 " +
			"AND _ut=''" +
			"THEN UPDATE SET " +
			"`col1`=_d._c0," +
			"`col2`=_d._c1," +
			"`col3`=_d._c2," +
			"`synced_at`=CURRENT_TIMESTAMP," +
			"`deleted`=FALSE",
		"WHEN MATCHED AND" +
			"_rt=2 AND _ut=''" +
			"THEN UPDATE SET `col1`=_d._c0,`col2`=_d._c1, " +
			"`col3`=_d._c2,`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE",
	}

	result := m.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestGenerateUpdateStatement_WithUnchangedToastCols(t *testing.T) {
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{"", "col2,col3", "col2", "col3"}
	m := &mergeStmtGenerator{
		shortColumn: map[string]string{
			"col1": "_c0",
			"col2": "_c1",
			"col3": "_c2",
		},
		peerdbCols: &protos.PeerDBColumns{
			SoftDeleteColName: "",
			SyncedAtColName:   "synced_at",
		},
	}

	expected := []string{
		"WHEN MATCHED AND _rt!=2 AND _ut=''" +
			" THEN UPDATE SET `col1`=_d._c0,`col2`=_d._c1,`col3`=_d._c2," +
			"`synced_at`=CURRENT_TIMESTAMP",
		"WHEN MATCHED AND _rt!=2 AND _ut='col2,col3' " +
			"THEN UPDATE SET `col1`=_d._c0,`synced_at`=CURRENT_TIMESTAMP",
		"WHEN MATCHED AND _rt!=2 " +
			"AND _ut='col2' " +
			"THEN UPDATE SET `col1`=_d._c0,`col3`=_d._c2," +
			"`synced_at`=CURRENT_TIMESTAMP",
		"WHEN MATCHED AND _rt!=2 AND _ut='col3' " +
			"THEN UPDATE SET `col1`=_d._c0," +
			"`col2`=_d._c1,`synced_at`=CURRENT_TIMESTAMP",
	}

	result := m.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v,\nbut got: %v", expected, result)
	}
}

func TestGenerateUpdateStatement_WithUnchangedToastColsAndSoftDelete(t *testing.T) {
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{"", "col2,col3", "col2", "col3"}
	m := &mergeStmtGenerator{
		shortColumn: map[string]string{
			"col1": "_c0",
			"col2": "_c1",
			"col3": "_c2",
		},
		peerdbCols: &protos.PeerDBColumns{
			SoftDeleteColName: "deleted",
			SyncedAtColName:   "synced_at",
		},
	}

	expected := []string{
		"WHEN MATCHED AND _rt!=2 AND _ut=''" +
			" THEN UPDATE SET `col1`=_d._c0,`col2`=_d._c1,`col3`=_d._c2," +
			"`synced_at`=CURRENT_TIMESTAMP,`deleted`=FALSE",
		"WHEN MATCHED AND _rt=2 " +
			"AND _ut='' " +
			"THEN UPDATE SET `col1`=_d._c0,`col2`=_d._c1," +
			"`col3`=_d._c2,`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE",
		"WHEN MATCHED AND _rt!=2 AND _ut='col2,col3' " +
			"THEN UPDATE SET `col1`=_d._c0,`synced_at`=CURRENT_TIMESTAMP,`deleted`=FALSE ",
		"WHEN MATCHED AND _rt=2 AND _ut='col2,col3' " +
			"THEN UPDATE SET `col1`=_d._c0,`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE",
		"WHEN MATCHED AND _rt!=2 " +
			"AND _ut='col2' " +
			"THEN UPDATE SET `col1`=_d._c0,`col3`=_d._c2," +
			"`synced_at`=CURRENT_TIMESTAMP,`deleted`=FALSE",
		"WHEN MATCHED AND _rt=2 " +
			"AND _ut='col2' " +
			"THEN UPDATE SET `col1`=_d._c0,`col3`=_d._c2," +
			"`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE ",
		"WHEN MATCHED AND _rt!=2 AND _ut='col3' " +
			"THEN UPDATE SET `col1`=_d._c0," +
			"`col2`=_d._c1,`synced_at`=CURRENT_TIMESTAMP,`deleted`=FALSE ",
		"WHEN MATCHED AND _rt=2 AND _ut='col3' " +
			"THEN UPDATE SET `col1`=_d._c0," +
			"`col2`=_d._c1,`synced_at`=CURRENT_TIMESTAMP,`deleted`=TRUE",
	}

	result := m.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v,\nbut got: %v", expected, result)
	}
}
