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

func TestCompareSemanticsRenameTableIsCaseSensitive(t *testing.T) {
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind:  "rename_table",
		Pairs: []ParsedPair{{OldTable: "fixture", NewTable: "fast"}},
	}}}
	actual := Delta{Renamed: []RenameSummary{{Old: "fixture", New: "FAST"}}}

	if got := CompareSemantics(SemanticInput{Actual: actual}, parsed); len(got) != 1 {
		t.Fatalf("case-sensitive rename findings = %v, want one", got)
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

func TestCompareSemanticsSameNameDropAddIsReplacement(t *testing.T) {
	beforeCol := ColRow{
		Name: "vector2", Ordinal: 3, ColumnType: "tinyint(1)", IsNullable: "YES",
		NumPrec: compareInt64Ptr(3), NumScale: compareInt64Ptr(0),
	}
	afterCol := ColRow{
		Name: "vector2", Ordinal: 32, ColumnType: "int(11)", IsNullable: "NO",
		NumPrec: compareInt64Ptr(10), NumScale: compareInt64Ptr(0),
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind: "alter_table",
		Specs: []ParsedSpec{
			{Op: "drop", OldName: "vector2"},
			{
				Op: "add",
				Cols: []ParsedCol{{
					Name: "vector2", TypeStr: "int", NotNull: true,
					Precision: -1, Scale: -1,
				}},
			},
		},
	}}}
	actual := Delta{Changed: []ColumnChange{{Name: "vector2", Before: beforeCol, After: afterCol}}}

	got := CompareSemantics(SemanticInput{
		Before: Snapshot{"vector2": beforeCol},
		After:  Snapshot{"vector2": afterCol},
		Actual: actual,
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("same-name drop/add findings = %v, want none", got)
	}
}

func TestCompareSemanticsSameNameRenameIsNoop(t *testing.T) {
	col := ColRow{Name: "n6", Ordinal: 34, ColumnType: "timestamp(6)", IsNullable: "YES"}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind:  "alter_table",
		Specs: []ParsedSpec{{Op: "rename_col", OldName: "n6", NewName: "n6"}},
	}}}

	got := CompareSemantics(SemanticInput{
		Before: Snapshot{"n6": col},
		After:  Snapshot{"n6": col},
		Actual: Delta{},
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("same-name rename findings = %v, want none", got)
	}
}

func TestCompareSemanticsRenameIntoChangedAwayName(t *testing.T) {
	before := Snapshot{
		"system2": {Name: "system2", Ordinal: 33, ColumnType: "int(11)", IsNullable: "YES", NumPrec: compareInt64Ptr(10), NumScale: compareInt64Ptr(0)},
		"c_blob":  {Name: "c_blob", Ordinal: 8, ColumnType: "blob", IsNullable: "YES"},
	}
	after := Snapshot{
		"system2":  {Name: "system2", Ordinal: 8, ColumnType: "blob", IsNullable: "YES"},
		"new`tick": {Name: "new`tick", Ordinal: 33, ColumnType: "double", IsNullable: "YES", NumPrec: compareInt64Ptr(22)},
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind: "alter_table",
		Specs: []ParsedSpec{
			{
				Op:      "change",
				OldName: "system2",
				Cols:    []ParsedCol{{Name: "new`tick", TypeStr: "double", Precision: -1, Scale: -1}},
			},
			{Op: "rename_col", OldName: "c_blob", NewName: "system2"},
		},
	}}}

	got := CompareSemantics(SemanticInput{
		Before: before,
		After:  after,
		Actual: DiffSnapshots(before, after),
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("rename into changed-away name findings = %v, want none", got)
	}
}

func TestDiffSnapshotsIgnoresColumnKey(t *testing.T) {
	before := Snapshot{"c": {Name: "c", Ordinal: 1, ColumnType: "int", IsNullable: "YES"}}
	after := Snapshot{"c": {Name: "c", Ordinal: 1, ColumnType: "int", IsNullable: "YES", ColumnKey: "MUL"}}

	if got := DiffSnapshots(before, after); !got.Empty() {
		t.Fatalf("DiffSnapshots column-key-only delta = %+v, want empty", got)
	}
}

func compareInt64Ptr(v int64) *int64 {
	return &v
}
