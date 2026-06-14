package connbigquery

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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

// the raw-table filter must use the legacy dotted destination name — the exact string
// the sync side writes into _peerdb_destination_table_name — including for dotted
// name components arriving first-dot-split from legacy configs
func TestGenerateFlattenedCTERawNameFilterIsLegacyDotted(t *testing.T) {
	m := &mergeStmtGenerator{
		shortColumn:     map[string]string{"id": "_c0"},
		rawDatasetTable: datasetTable{dataset: "ds", table: "_peerdb_raw_my_mirror"},
		mergeBatchId:    7,
	}
	schema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{{Name: "id", Type: string(types.QValueKindInt64)}},
	}

	for _, tc := range []struct {
		dstTable common.QualifiedTable
		rawName  string
	}{
		{dstTable: common.QualifiedTable{Namespace: "ds", Table: "my_table"}, rawName: "ds.my_table"},
		{dstTable: common.QualifiedTable{Namespace: "sch.ema", Table: "ta.ble"}, rawName: "sch.ema.ta.ble"},
	} {
		cte := m.generateFlattenedCTE(tc.dstTable, schema)
		want := fmt.Sprintf("_peerdb_destination_table_name='%s'", tc.rawName)
		if !strings.Contains(cte, want) {
			t.Errorf("flattened CTE filter for %v should contain %q, got: %s", tc.dstTable, want, cte)
		}
	}
}
