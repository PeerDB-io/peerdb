package connpostgres

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestGenerateMergeUpdateStatement(t *testing.T) {
	allCols := []string{`"col1"`, `"col2"`, `"col3"`}
	unchangedToastCols := []string{""}

	expected := []string{
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns=''
		THEN UPDATE SET "col1"=src."col1","col2"=src."col2","col3"=src."col3",
		 "_peerdb_synced_at"=CURRENT_TIMESTAMP`,
	}
	normalizeGen := normalizeStmtGenerator{
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "",
		},
	}
	result := normalizeGen.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestGenerateMergeUpdateStatement_WithSoftDelete(t *testing.T) {
	allCols := []string{`"col1"`, `"col2"`, `"col3"`}
	unchangedToastCols := []string{""}

	expected := []string{
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns=''
		THEN UPDATE SET "col1"=src."col1","col2"=src."col2","col3"=src."col3",
		 "_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=FALSE`,
		`WHEN MATCHED AND src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns=''
		 THEN UPDATE SET "col1"=src."col1","col2"=src."col2","col3"=src."col3",
		  "_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=TRUE`,
	}
	normalizeGen := normalizeStmtGenerator{
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "_peerdb_soft_delete",
		},
	}
	result := normalizeGen.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestGenerateMergeUpdateStatement_WithUnchangedToastCols(t *testing.T) {
	allCols := []string{`"col1"`, `"col2"`, `"col3"`}
	unchangedToastCols := []string{"", "col2,col3", "col2", "col3"}

	expected := []string{
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns=''
		 THEN UPDATE SET "col1"=src."col1","col2"=src."col2","col3"=src."col3","_peerdb_synced_at"=CURRENT_TIMESTAMP`,
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns='col2,col3'
		 THEN UPDATE SET "col1"=src."col1","_peerdb_synced_at"=CURRENT_TIMESTAMP`,
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns='col2'
		 THEN UPDATE SET "col1"=src."col1","col3"=src."col3","_peerdb_synced_at"=CURRENT_TIMESTAMP`,
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns='col3'
		 THEN UPDATE SET "col1"=src."col1","col2"=src."col2","_peerdb_synced_at"=CURRENT_TIMESTAMP`,
	}
	normalizeGen := normalizeStmtGenerator{
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "",
		},
	}
	result := normalizeGen.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

func TestGenerateMergeUpdateStatement_WithUnchangedToastColsAndSoftDelete(t *testing.T) {
	allCols := []string{`"col1"`, `"col2"`, `"col3"`}
	unchangedToastCols := []string{"", "col2,col3", "col2", "col3"}

	expected := []string{
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns=''
		 THEN UPDATE SET "col1"=src."col1","col2"=src."col2","col3"=src."col3",
		 "_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=FALSE`,
		`WHEN MATCHED AND src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns=''
		 THEN UPDATE SET "col1"=src."col1","col2"=src."col2","col3"=src."col3",
		 "_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=TRUE`,
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns='col2,col3'
		 THEN UPDATE SET "col1"=src."col1","_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=FALSE`,
		`WHEN MATCHED AND src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns='col2,col3'
		 THEN UPDATE SET "col1"=src."col1","_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=TRUE`,
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns='col2'
		 THEN UPDATE SET "col1"=src."col1","col3"=src."col3","_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=FALSE`,
		`WHEN MATCHED AND src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns='col2'
		 THEN UPDATE SET "col1"=src."col1","col3"=src."col3","_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=TRUE`,
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns='col3'
		 THEN UPDATE SET "col1"=src."col1","col2"=src."col2","_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=FALSE`,
		`WHEN MATCHED AND src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns='col3'
		 THEN UPDATE SET "col1"=src."col1","col2"=src."col2","_peerdb_synced_at"=CURRENT_TIMESTAMP,"_peerdb_soft_delete"=TRUE`,
	}
	normalizeGen := normalizeStmtGenerator{
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "_peerdb_soft_delete",
		},
	}
	result := normalizeGen.generateUpdateStatements(allCols, unchangedToastCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}

// helper to build a simple PG-typed TableSchema for merge tests
func buildTableSchema(columns []*protos.FieldDescription, pks []string) *protos.TableSchema {
	return &protos.TableSchema{
		TableIdentifier:   "test_table",
		System:            protos.TypeSystem_PG,
		PrimaryKeyColumns: pks,
		Columns:           columns,
	}
}

func normalizeSQL(s string) string {
	return utils.RemoveSpacesTabsNewlines(s)
}

func TestGenerateMergeStatement_BasicColumns(t *testing.T) {
	schema := buildTableSchema([]*protos.FieldDescription{
		{Name: "id", Type: "integer"},
		{Name: "name", Type: "text"},
		{Name: "value", Type: "numeric"},
	}, []string{"id"})

	gen := normalizeStmtGenerator{
		rawTableName:             "_peerdb_raw_test",
		tableSchemaMapping:       map[string]*protos.TableSchema{"public.test_table": schema},
		unchangedToastColumnsMap: map[string][]string{"public.test_table": {""}},
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "",
		},
		metadataSchema: "_peerdb_internal",
		supportsMerge:  true,
	}

	result := gen.generateMergeStatement("public.test_table", schema, []string{""})

	// CTE uses jsonb_to_record with record definitions
	require.Contains(t, result, "jsonb_to_record(_peerdb_data)")
	require.Contains(t, result, `"id" integer`)
	require.Contains(t, result, `"name" text`)
	require.Contains(t, result, `"value" numeric`)
	// USING select just references the column names directly (no ->> casts)
	require.Contains(t, normalizeSQL(result), normalizeSQL(`"id","name","value",_peerdb_record_type,_peerdb_unchanged_toast_columns`))
	// MERGE ON condition
	require.Contains(t, result, `src."id"=dst."id"`)
	require.NotContains(t, result, "_peerdb_data->>")
}

func TestGenerateMergeStatement_JsonColumns(t *testing.T) {
	schema := buildTableSchema([]*protos.FieldDescription{
		{Name: "id", Type: "integer"},
		{Name: "metadata", Type: "json"},
		{Name: "config", Type: "jsonb"},
	}, []string{"id"})

	gen := normalizeStmtGenerator{
		rawTableName:             "_peerdb_raw_test",
		tableSchemaMapping:       map[string]*protos.TableSchema{"public.test_table": schema},
		unchangedToastColumnsMap: map[string][]string{"public.test_table": {""}},
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "",
		},
		metadataSchema: "_peerdb_internal",
		supportsMerge:  true,
	}

	result := gen.generateMergeStatement("public.test_table", schema, []string{""})

	// json/jsonb columns: record defs should declare them as jsonb
	require.Contains(t, result, `"metadata" jsonb`)
	require.Contains(t, result, `"config" jsonb`)
	// USING select should unwrap them via #>>
	require.Contains(t, normalizeSQL(result), normalizeSQL(`("metadata" #>> '{}')::json AS "metadata"`))
	require.Contains(t, normalizeSQL(result), normalizeSQL(`("config" #>> '{}')::jsonb AS "config"`))
}

func TestGenerateMergeStatement_SoftDelete(t *testing.T) {
	schema := buildTableSchema([]*protos.FieldDescription{
		{Name: "id", Type: "integer"},
		{Name: "data", Type: "text"},
	}, []string{"id"})

	gen := normalizeStmtGenerator{
		rawTableName:             "_peerdb_raw_test",
		tableSchemaMapping:       map[string]*protos.TableSchema{"public.test_table": schema},
		unchangedToastColumnsMap: map[string][]string{"public.test_table": {""}},
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "_peerdb_soft_delete",
		},
		metadataSchema: "_peerdb_internal",
		supportsMerge:  true,
	}

	result := gen.generateMergeStatement("public.test_table", schema, []string{""})

	// soft delete: WHEN NOT MATCHED with record_type=2 inserts with soft delete TRUE
	require.Contains(t, normalizeSQL(result),
		normalizeSQL(`WHEN NOT MATCHED AND (src._peerdb_record_type=2) THEN INSERT`))
	require.Contains(t, normalizeSQL(result), normalizeSQL(`"_peerdb_soft_delete"`))
	// conflict part should be UPDATE SET soft_delete=TRUE
	require.Contains(t, normalizeSQL(result),
		normalizeSQL(`UPDATE SET "_peerdb_soft_delete"=TRUE,"_peerdb_synced_at"=CURRENT_TIMESTAMP`))
}

func TestGenerateMergeStatement_CompositePK(t *testing.T) {
	schema := buildTableSchema([]*protos.FieldDescription{
		{Name: "tenant_id", Type: "integer"},
		{Name: "user_id", Type: "integer"},
		{Name: "email", Type: "text"},
	}, []string{"tenant_id", "user_id"})

	gen := normalizeStmtGenerator{
		rawTableName:             "_peerdb_raw_test",
		tableSchemaMapping:       map[string]*protos.TableSchema{"public.test_table": schema},
		unchangedToastColumnsMap: map[string][]string{"public.test_table": {""}},
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "",
		},
		metadataSchema: "_peerdb_internal",
		supportsMerge:  true,
	}

	result := gen.generateMergeStatement("public.test_table", schema, []string{""})

	// composite PK: PARTITION BY should use both PK columns
	require.Contains(t, normalizeSQL(result), normalizeSQL(`PARTITION BY "tenant_id","user_id"`))
	// ON clause should join on both
	require.Contains(t, normalizeSQL(result), normalizeSQL(`src."tenant_id"=dst."tenant_id"`))
	require.Contains(t, normalizeSQL(result), normalizeSQL(`src."user_id"=dst."user_id"`))
}

func TestGenerateMergeStatement_ToastColumns(t *testing.T) {
	schema := buildTableSchema([]*protos.FieldDescription{
		{Name: "id", Type: "integer"},
		{Name: "small_col", Type: "text"},
		{Name: "big_col", Type: "text"},
	}, []string{"id"})

	gen := normalizeStmtGenerator{
		rawTableName:             "_peerdb_raw_test",
		tableSchemaMapping:       map[string]*protos.TableSchema{"public.test_table": schema},
		unchangedToastColumnsMap: map[string][]string{"public.test_table": {"", "big_col"}},
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "",
		},
		metadataSchema: "_peerdb_internal",
		supportsMerge:  true,
	}

	result := gen.generateMergeStatement("public.test_table", schema, []string{"", "big_col"})

	normalized := normalizeSQL(result)
	// Should have an update branch for unchanged toast col = '' (all cols updated)
	require.Contains(t, normalized, normalizeSQL(`_peerdb_unchanged_toast_columns=''`))
	// Should have an update branch for unchanged toast col = 'big_col' (only id and small_col updated)
	require.Contains(t, normalized, normalizeSQL(`_peerdb_unchanged_toast_columns='big_col'`))
	require.Contains(t, normalized, normalizeSQL(`"id"=src."id","small_col"=src."small_col"`))
}

func TestGenerateMergeStatement_UserDefinedType(t *testing.T) {
	schema := buildTableSchema([]*protos.FieldDescription{
		{Name: "id", Type: "integer"},
		{Name: "status", Type: "my_enum", TypeSchemaName: "my_schema"},
	}, []string{"id"})

	gen := normalizeStmtGenerator{
		rawTableName:             "_peerdb_raw_test",
		tableSchemaMapping:       map[string]*protos.TableSchema{"public.test_table": schema},
		unchangedToastColumnsMap: map[string][]string{"public.test_table": {""}},
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "",
		},
		metadataSchema: "_peerdb_internal",
		supportsMerge:  true,
	}

	result := gen.generateMergeStatement("public.test_table", schema, []string{""})

	// User-defined types should be schema-qualified in the record definition
	require.Contains(t, result, `"status" "my_schema"."my_enum"`)
}

func TestGenerateNormalizeStatements_Merge(t *testing.T) {
	schema := buildTableSchema([]*protos.FieldDescription{
		{Name: "id", Type: "integer"},
		{Name: "name", Type: "text"},
	}, []string{"id"})

	gen := normalizeStmtGenerator{
		rawTableName:             "_peerdb_raw_test",
		tableSchemaMapping:       map[string]*protos.TableSchema{"public.test_table": schema},
		unchangedToastColumnsMap: map[string][]string{"public.test_table": {""}},
		peerdbCols: &protos.PeerDBColumns{
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "",
		},
		metadataSchema: "_peerdb_internal",
		supportsMerge:  true,
	}

	stmts := gen.generateNormalizeStatements("public.test_table")
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0], "jsonb_to_record")
	require.Contains(t, stmts[0], "MERGE INTO")
}
