package connpostgres

import (
	"reflect"
	"testing"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func TestGenerateMergeUpdateStatement(t *testing.T) {
	allCols := []string{`"col1"`, `"col2"`, `"col3"`}
	unchangedToastCols := []string{""}

	expected := []string{
		`WHEN MATCHED AND src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns=''
		THEN UPDATE SET "col1"=src."col1","col2"=src."col2","col3"=src."col3",
		 "_peerdb_synced_at"=CURRENT_TIMESTAMP`,
	}
	normalizeGen := &normalizeStmtGenerator{
		unchangedToastColumns: unchangedToastCols,
		peerdbCols: &protos.PeerDBColumns{
			SoftDelete:        false,
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "_peerdb_soft_delete",
		},
	}
	result := normalizeGen.generateUpdateStatements(allCols)

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
	normalizeGen := &normalizeStmtGenerator{
		unchangedToastColumns: unchangedToastCols,
		peerdbCols: &protos.PeerDBColumns{
			SoftDelete:        true,
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "_peerdb_soft_delete",
		},
	}
	result := normalizeGen.generateUpdateStatements(allCols)

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
	normalizeGen := &normalizeStmtGenerator{
		unchangedToastColumns: unchangedToastCols,
		peerdbCols: &protos.PeerDBColumns{
			SoftDelete:        false,
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "_peerdb_soft_delete",
		},
	}
	result := normalizeGen.generateUpdateStatements(allCols)

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
	normalizeGen := &normalizeStmtGenerator{
		unchangedToastColumns: unchangedToastCols,
		peerdbCols: &protos.PeerDBColumns{
			SoftDelete:        true,
			SyncedAtColName:   "_peerdb_synced_at",
			SoftDeleteColName: "_peerdb_soft_delete",
		},
	}
	result := normalizeGen.generateUpdateStatements(allCols)

	for i := range expected {
		expected[i] = utils.RemoveSpacesTabsNewlines(expected[i])
		result[i] = utils.RemoveSpacesTabsNewlines(result[i])
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}
