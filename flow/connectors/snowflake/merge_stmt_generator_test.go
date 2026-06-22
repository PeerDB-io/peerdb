package connsnowflake

import (
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

	expected := []string{
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP`,
	}
	mergeGen := &mergeStmtGenerator{
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_PEERDB_SYNCED_AT",
			SoftDeleteColName: "",
		},
	}
	result := mergeGen.generateUpdateStatements(allCols, unchangedToastCols)

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

	expected := []string{
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		 THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2", "COL3" = SOURCE."COL3",
		  "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = TRUE`,
	}
	mergeGen := &mergeStmtGenerator{
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_PEERDB_SYNCED_AT",
			SoftDeleteColName: "_PEERDB_SOFT_DELETE",
		},
	}
	result := mergeGen.generateUpdateStatements(allCols, unchangedToastCols)

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

	expected := []string{
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col2,col3'
		THEN UPDATE SET "COL1" = SOURCE."COL1",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col2'
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col3'
		THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP`,
	}
	mergeGen := &mergeStmtGenerator{
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_PEERDB_SYNCED_AT",
			SoftDeleteColName: "",
		},
	}
	result := mergeGen.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestGenerateUpdateStatement_WithUnchangedToastColsAndSoftDelete(t *testing.T) {
	allCols := []string{"col1", "col2", "col3"}
	unchangedToastCols := []string{"", "col2,col3", "col2", "col3"}

	expected := []string{
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		 THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		 THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2", "COL3" = SOURCE."COL3",
		  "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = TRUE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col2,col3'
		 THEN UPDATE SET "COL1" = SOURCE."COL1",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col2,col3'
		 THEN UPDATE SET "COL1" = SOURCE."COL1",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = TRUE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col2'
		 THEN UPDATE SET "COL1" = SOURCE."COL1", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col2'
		 THEN UPDATE SET "COL1" = SOURCE."COL1", "COL3" = SOURCE."COL3",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = TRUE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col3'
		 THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = FALSE`,
		`WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='col3'
		 THEN UPDATE SET "COL1" = SOURCE."COL1", "COL2" = SOURCE."COL2",
		 "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_SOFT_DELETE" = TRUE`,
	}
	mergeGen := &mergeStmtGenerator{
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_PEERDB_SYNCED_AT",
			SoftDeleteColName: "_PEERDB_SOFT_DELETE",
		},
	}
	result := mergeGen.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

// goldenDotlessMergeStmt is the exact statement the pre-QualifiedTable generator
// (merge-base 0709374b) produced for destination "public.myTable" with the schema in
// TestGenerateMergeStmtGolden; the refactored generator must reproduce it byte-for-byte
// so existing mirrors see unchanged SQL.
//
//nolint:lll
const goldenDotlessMergeStmt = `MERGE INTO "PUBLIC"."myTable" TARGET USING (WITH VARIANT_CONVERTED AS (
		SELECT _PEERDB_UID,_PEERDB_TIMESTAMP,TO_VARIANT(PARSE_JSON(_PEERDB_DATA)) VAR_COLS,_PEERDB_RECORD_TYPE,
		 _PEERDB_MATCH_DATA,_PEERDB_BATCH_ID,_PEERDB_UNCHANGED_TOAST_COLUMNS
		FROM _PEERDB_INTERNAL._PEERDB_RAW_my_mirror WHERE _PEERDB_BATCH_ID = 42 AND
		 _PEERDB_DATA != '' AND
		 _PEERDB_DESTINATION_TABLE_NAME = ? ), FLATTENED AS
		 (SELECT _PEERDB_UID,_PEERDB_TIMESTAMP,_PEERDB_RECORD_TYPE,_PEERDB_MATCH_DATA,_PEERDB_BATCH_ID,
			_PEERDB_UNCHANGED_TOAST_COLUMNS,CAST(VAR_COLS:"id" AS INTEGER) AS "ID",CAST(VAR_COLS:"myData" AS STRING) AS "myData"
		 FROM VARIANT_CONVERTED), DEDUPLICATED_FLATTENED AS (SELECT _PEERDB_RANKED.* FROM
		 (SELECT RANK() OVER
		 (PARTITION BY ("ID") ORDER BY _PEERDB_TIMESTAMP DESC) AS _PEERDB_RANK, * FROM FLATTENED)
		 _PEERDB_RANKED WHERE _PEERDB_RANK = 1)
		 SELECT * FROM DEDUPLICATED_FLATTENED) SOURCE ON TARGET."ID" = SOURCE."ID"
		 WHEN NOT MATCHED AND (SOURCE._PEERDB_RECORD_TYPE != 2) THEN INSERT ("ID","myData","_PEERDB_SYNCED_AT") VALUES(SOURCE."ID",SOURCE."myData",CURRENT_TIMESTAMP)
		 WHEN MATCHED AND
		(SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
		THEN UPDATE SET "ID" = SOURCE."ID", "myData" = SOURCE."myData", "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_IS_DELETED" = FALSE  WHEN MATCHED AND
			(SOURCE._PEERDB_RECORD_TYPE = 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS=''
			THEN UPDATE SET "ID" = SOURCE."ID", "myData" = SOURCE."myData", "_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP, "_PEERDB_IS_DELETED" = TRUE  WHEN NOT MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) THEN INSERT ("ID","myData","_PEERDB_SYNCED_AT",_PEERDB_IS_DELETED) VALUES(SOURCE."ID",SOURCE."myData",CURRENT_TIMESTAMP,TRUE)
		 WHEN MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) THEN UPDATE SET _PEERDB_IS_DELETED = TRUE, _PEERDB_SYNCED_AT = CURRENT_TIMESTAMP`

func TestGenerateMergeStmtGolden(t *testing.T) {
	schema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
			{Name: "myData", Type: string(types.QValueKindString)},
		},
		PrimaryKeyColumns: []string{"id"},
	}

	newGenerator := func(dstTable common.QualifiedTable, rawName string) *mergeStmtGenerator {
		return &mergeStmtGenerator{
			tableSchemaMapping:       map[common.QualifiedTable]*protos.TableSchema{dstTable: schema},
			unchangedToastColumnsMap: map[string][]string{rawName: {""}},
			peerdbCols: &protos.PeerDBColumns{
				SyncedAtColName:   "_PEERDB_SYNCED_AT",
				SoftDeleteColName: "_PEERDB_IS_DELETED",
			},
			rawTableName: "_PEERDB_RAW_my_mirror",
			mergeBatchId: 42,
		}
	}

	t.Run("dotless byte-identical to pre-refactor", func(t *testing.T) {
		dstTable := common.QualifiedTable{Namespace: "public", Table: "myTable"}
		stmt, err := newGenerator(dstTable, "public.myTable").generateMergeStmt(t.Context(), nil, dstTable)
		if err != nil {
			t.Fatal(err)
		}
		if stmt != goldenDotlessMergeStmt {
			t.Errorf("merge statement diverged from pre-QualifiedTable output:\ngot:\n%s\nwant:\n%s", stmt, goldenDotlessMergeStmt)
		}
	})

	t.Run("dotted components quoted per-component", func(t *testing.T) {
		dstTable := common.QualifiedTable{Namespace: "sch.ema", Table: "ta.ble"}
		stmt, err := newGenerator(dstTable, "sch.ema.ta.ble").generateMergeStmt(t.Context(), nil, dstTable)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(stmt, "MERGE INTO \"SCH.EMA\".\"TA.BLE\" TARGET") {
			t.Errorf("dotted destination not quoted per-component: %s", stmt[:80])
		}
		// the unchanged-toast map is keyed by the raw-table string (LegacyDotted of the
		// destination); a quoted-String() or .Table key would miss and silently drop
		// the toast-handling UPDATE branch from the statement
		if !strings.Contains(stmt, "_PEERDB_UNCHANGED_TOAST_COLUMNS=''") {
			t.Error("LegacyDotted-keyed unchangedToastColumnsMap lookup missed; toast UPDATE branch absent")
		}
	})
}
