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

func TestCompareSemanticsAlterRenameTableIgnoresMovedColumnSnapshot(t *testing.T) {
	before := Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint(20)", IsNullable: "NO", ColumnKey: "PRI"},
		"c":  {Name: "c", Ordinal: 2, ColumnType: "int(11)", IsNullable: "YES"},
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{
		{
			Kind:  "alter_table",
			Table: "fixture",
			Specs: []ParsedSpec{{
				Op:      "change",
				OldName: "id",
				Cols: []ParsedCol{{
					Name: "n1", TypeStr: "varchar(12)", Precision: -1, Scale: -1,
				}},
				HasPosition: true,
			}},
		},
		{
			Kind:  "rename_table",
			Pairs: []ParsedPair{{OldTable: "fixture", NewTable: "fixture_r"}},
		},
	}}
	actual := Delta{
		Dropped: []ColRow{before["id"], before["c"]},
		Renamed: []RenameSummary{{Old: "fixture", New: "fixture_r"}},
	}

	got := CompareSemantics(SemanticInput{
		Before: before,
		After:  Snapshot{},
		Actual: actual,
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("alter rename findings = %v, want none", got)
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

func TestCompareSemanticsChangeFromMissingOldNameProducesNewColumn(t *testing.T) {
	afterCol := ColRow{
		Name: "n8", Ordinal: 1, ColumnType: "decimal(10,0)", IsNullable: "YES",
		NumPrec: compareInt64Ptr(10), NumScale: compareInt64Ptr(0),
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind: "alter_table",
		Specs: []ParsedSpec{
			{
				Op:      "change",
				OldName: "fixture",
				Cols: []ParsedCol{{
					Name: "n8", TypeStr: "decimal(10)", Precision: 10, Scale: -1,
				}},
			},
		},
	}}}
	actual := Delta{Added: []ColRow{afterCol}}

	got := CompareSemantics(SemanticInput{
		Before: Snapshot{},
		After:  Snapshot{"n8": afterCol},
		Actual: actual,
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("change from missing old-name findings = %v, want none", got)
	}
}

func TestCompareSemanticsIfExistsMissingOldNameIsNoop(t *testing.T) {
	before := Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint(20) unsigned", IsNullable: "NO", ColumnKey: "PRI"},
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind: "alter_table",
		Specs: []ParsedSpec{
			{
				Op:       "change",
				OldName:  "fixture",
				Cols:     []ParsedCol{{Name: "n3", TypeStr: "binary(8)", Precision: -1, Scale: -1}},
				IfExists: true,
			},
			{Op: "drop", OldName: "missing", IfExists: true},
			{Op: "rename_col", OldName: "absent", NewName: "n4", IfExists: true},
		},
	}}}

	got := CompareSemantics(SemanticInput{
		Before: before,
		After:  before,
		Actual: Delta{},
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("conditional no-op findings = %v, want none", got)
	}
}

func TestCompareSemanticsIfExistsPresentOldNameApplies(t *testing.T) {
	before := Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint(20)", IsNullable: "NO", ColumnKey: "PRI"},
	}
	after := Snapshot{
		"n2": {Name: "n2", Ordinal: 1, ColumnType: "bigint(20) unsigned", IsNullable: "NO", ColumnKey: "PRI"},
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind: "alter_table",
		Specs: []ParsedSpec{{
			Op:       "change",
			OldName:  "id",
			Cols:     []ParsedCol{{Name: "n2", TypeStr: "bigint unsigned", NotNull: true, Precision: -1, Scale: -1}},
			IfExists: true,
		}},
	}}}

	got := CompareSemantics(SemanticInput{
		Before: before,
		After:  after,
		Actual: DiffSnapshots(before, after),
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("conditional existing-column findings = %v, want none", got)
	}
}

func TestCompareSemanticsIfNotExistsPresentColumnIsNoop(t *testing.T) {
	before := Snapshot{
		"fixture": {Name: "fixture", Ordinal: 1, ColumnType: "longtext", IsNullable: "YES"},
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind: "alter_table",
		Specs: []ParsedSpec{{
			Op:          "add",
			Cols:        []ParsedCol{{Name: "fixture", TypeStr: "char(8)", NotNull: true, Precision: -1, Scale: -1}},
			IfNotExists: true,
		}},
	}}}

	got := CompareSemantics(SemanticInput{
		Before: before,
		After:  before,
		Actual: Delta{},
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("conditional add no-op findings = %v, want none", got)
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

func TestCompareSemanticsAddPrimaryKeyImpliedNotNullTolerated(t *testing.T) {
	before := Snapshot{
		"id":     {Name: "id", Ordinal: 1, ColumnType: "bigint(20)", IsNullable: "NO", ColumnKey: "PRI"},
		"period": {Name: "period", Ordinal: 2, ColumnType: "int(11)", IsNullable: "YES", NumPrec: compareInt64Ptr(10), NumScale: compareInt64Ptr(0)},
	}
	after := Snapshot{
		"period": {Name: "period", Ordinal: 1, ColumnType: "int(11)", IsNullable: "NO", ColumnKey: "PRI", NumPrec: compareInt64Ptr(10), NumScale: compareInt64Ptr(0)},
		"n1":     {Name: "n1", Ordinal: 2, ColumnType: "blob", IsNullable: "YES"},
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind:  "alter_table",
		Table: "fixture",
		Specs: []ParsedSpec{
			{Op: "add", Cols: []ParsedCol{{Name: "n1", TypeStr: "blob", Precision: -1, Scale: -1}}},
			{Op: "drop", OldName: "id"},
		},
	}}}

	got := CompareSemantics(SemanticInput{
		Before: before,
		After:  after,
		Actual: DiffSnapshots(before, after),
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("pk-implied not-null findings = %v, want none", got)
	}
}

func TestCompareSemanticsUnexpectedNullabilityChangeWithoutPKStillFires(t *testing.T) {
	before := Snapshot{
		"period": {Name: "period", Ordinal: 1, ColumnType: "int(11)", IsNullable: "YES"},
	}
	after := Snapshot{
		"period": {Name: "period", Ordinal: 1, ColumnType: "int(11)", IsNullable: "NO"},
		"n1":     {Name: "n1", Ordinal: 2, ColumnType: "blob", IsNullable: "YES"},
	}
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind:  "alter_table",
		Table: "fixture",
		Specs: []ParsedSpec{
			{Op: "add", Cols: []ParsedCol{{Name: "n1", TypeStr: "blob", Precision: -1, Scale: -1}}},
		},
	}}}

	got := CompareSemantics(SemanticInput{
		Before: before,
		After:  after,
		Actual: DiffSnapshots(before, after),
	}, parsed)
	if len(got) != 1 {
		t.Fatalf("non-pk nullability findings = %v, want one changed_unexpected", got)
	}
	if got[0].Meta["changed_unexpected"] != "period" {
		t.Fatalf("finding meta = %v, want changed_unexpected=period", got[0].Meta)
	}
}

func TestCompareSemanticsToleratesImplicitBinaryDefaultCharset(t *testing.T) {
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind: "alter_table",
		Specs: []ParsedSpec{{
			Op: "add",
			Cols: []ParsedCol{
				{Name: "n1", TypeStr: "varchar(32)", Precision: -1, Scale: -1},
				{Name: "n2", TypeStr: "text", Precision: -1, Scale: -1},
			},
		}},
	}}}
	after := Snapshot{
		"n1": {Name: "n1", Ordinal: 1, ColumnType: "varbinary(32)", IsNullable: "YES"},
		"n2": {Name: "n2", Ordinal: 2, ColumnType: "blob", IsNullable: "YES"},
	}

	got := CompareSemantics(SemanticInput{
		After:  after,
		Actual: DiffSnapshots(nil, after),
	}, parsed)
	if len(got) != 0 {
		t.Fatalf("implicit binary charset findings = %v, want none", got)
	}
}

func TestCompareSemanticsBinaryDefaultCharsetToleranceIsNarrow(t *testing.T) {
	parsed := ParsedStmts{Stmts: []ParsedStmt{{
		Kind: "alter_table",
		Specs: []ParsedSpec{{
			Op:   "add",
			Cols: []ParsedCol{{Name: "n1", TypeStr: "varchar(32)", Precision: -1, Scale: -1}},
		}},
	}}}
	after := Snapshot{
		"n1": {Name: "n1", Ordinal: 1, ColumnType: "blob", IsNullable: "YES"},
	}

	got := CompareSemantics(SemanticInput{
		After:  after,
		Actual: DiffSnapshots(nil, after),
	}, parsed)
	if len(got) != 1 || got[0].Class != ClassColumnAttr {
		t.Fatalf("narrow tolerance findings = %v, want one column attr finding", got)
	}
}

func compareInt64Ptr(v int64) *int64 {
	return &v
}
