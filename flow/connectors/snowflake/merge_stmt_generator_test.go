package connsnowflake

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestGenerateMergeStatementNullableSchemaDoesNotAddNotNullToCasts(t *testing.T) {
	destinationTable := "public.events"
	requiredTimestamp := &protos.FieldDescription{
		Name:     "created_at",
		Type:     string(types.QValueKindTimestampTZ),
		Nullable: false,
	}
	generator := &mergeStmtGenerator{
		tableSchemaMapping: map[string]*protos.TableSchema{
			destinationTable: {
				Columns: []*protos.FieldDescription{
					{Name: "id", Type: string(types.QValueKindInt64), Nullable: false},
					requiredTimestamp,
					{Name: "deleted_at", Type: string(types.QValueKindTimestampTZ), Nullable: true},
				},
				PrimaryKeyColumns: []string{"id"},
				NullableEnabled:   true,
			},
		},
		unchangedToastColumnsMap: map[string][]string{destinationTable: {""}},
		peerdbCols:               &protos.PeerDBColumns{},
		rawTableName:             "raw_events",
		mergeBatchId:             1,
	}

	statement, err := generator.generateMergeStmt(context.Background(), nil, destinationTable)
	if err != nil {
		t.Fatalf("generate merge statement: %v", err)
	}
	if strings.Contains(statement, "NOT NULL") {
		t.Fatalf("merge statement contains invalid NOT NULL cast: %s", statement)
	}
	for _, expected := range []string{
		`CAST(VAR_COLS:"created_at" AS TIMESTAMP_TZ) AS "CREATED_AT"`,
		`CAST(VAR_COLS:"deleted_at" AS TIMESTAMP_TZ) AS "DELETED_AT"`,
	} {
		if !strings.Contains(statement, expected) {
			t.Errorf("merge statement missing %q: %s", expected, statement)
		}
	}

	requiredType, err := qvalue.ToDWHColumnType(
		context.Background(), types.QValueKindTimestampTZ, nil, protos.DBType_SNOWFLAKE,
		nil, requiredTimestamp, true, nil,
	)
	if err != nil {
		t.Fatalf("convert required destination column type: %v", err)
	}
	if requiredType != "TIMESTAMP_TZ NOT NULL" {
		t.Fatalf("required destination column type = %q, want TIMESTAMP_TZ NOT NULL", requiredType)
	}
}

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
