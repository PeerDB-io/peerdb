package e2echeck

import "testing"

func TestCompareSemanticsRenameTableIgnoresColumnSnapshotDelta(t *testing.T) {
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind:  "rename_table",
		Pairs: []ParsedPair{{OldTable: "fixture", NewTable: "fixture_r"}},
	}}}
	actual := Delta{
		Dropped: []ColRow{
			{Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
			{Name: "c", Ordinal: 2, ColumnType: "int", IsNullable: "YES"},
		},
		Renamed: []RenameSummary{{Old: "fixture", New: "fixture_r"}},
	}

	if got := CompareSemantics(SemanticInput{Actual: actual}, parsed); len(got) != 0 {
		t.Fatalf("rename findings = %v, want none", got)
	}
}

func TestCompareSemanticsIgnoresRecordedColumnKeyOnlyChange(t *testing.T) {
	before := ColRow{Name: "c", Ordinal: 1, ColumnType: "int", IsNullable: "YES"}
	after := before
	after.ColumnKey = "MUL"
	parsed := ParsedStmts{Stmts: []ParsedStmt{{Kind: "alter_table", Table: "fixture"}}}
	actual := Delta{Changed: []ColumnChange{{Name: "c", Before: before, After: after}}}

	if got := CompareSemantics(SemanticInput{Actual: actual}, parsed); len(got) != 0 {
		t.Fatalf("column-key-only findings = %v, want none", got)
	}
}

func TestDiffSnapshotsIgnoresColumnKey(t *testing.T) {
	before := Snapshot{"c": {Name: "c", Ordinal: 1, ColumnType: "int", IsNullable: "YES"}}
	after := Snapshot{"c": {Name: "c", Ordinal: 1, ColumnType: "int", IsNullable: "YES", ColumnKey: "MUL"}}

	if got := DiffSnapshots(before, after); !got.Empty() {
		t.Fatalf("DiffSnapshots column-key-only delta = %+v, want empty", got)
	}
}
