package e2e

import (
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/e2echeck"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestCompareSemanticsOpsAndReconciliations(t *testing.T) {
	base := snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
		"c":  {Name: "c", Ordinal: 2, ColumnType: "int", IsNullable: "YES"},
		"d":  {Name: "d", Ordinal: 3, ColumnType: "int", IsNullable: "YES"},
	}
	tests := []struct {
		name   string
		before snapshot
		after  snapshot
		parsed parsedStmts
		want   []string
	}{
		{
			name:   "add",
			before: base,
			after:  withRows(base, colRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "YES"}),
			parsed: oneAlter(parsedSpec{Op: "add", Cols: []parsedCol{{Name: "n1", TypeStr: "int", Precision: -1, Scale: -1}}}),
		},
		{
			name:   "modify same name",
			before: base,
			after:  withReplace(base, colRow{Name: "c", Ordinal: 2, ColumnType: "bigint", IsNullable: "NO"}),
			parsed: oneAlter(parsedSpec{Op: "add", Cols: []parsedCol{{Name: "c", TypeStr: "bigint", NotNull: true, Precision: -1, Scale: -1}}}),
		},
		{
			name:   "change",
			before: base,
			after:  withoutRows(withRows(base, colRow{Name: "n1", Ordinal: 2, ColumnType: "bigint", IsNullable: "YES"}), "c"),
			parsed: oneAlter(parsedSpec{Op: "change", OldName: "c", Cols: []parsedCol{{Name: "n1", TypeStr: "bigint", Precision: -1, Scale: -1}}}),
		},
		{
			name:   "rename col",
			before: base,
			after:  withoutRows(withRows(base, colRow{Name: "n1", Ordinal: 2, ColumnType: "int", IsNullable: "YES"}), "c"),
			parsed: oneAlter(parsedSpec{Op: "rename_col", OldName: "c", NewName: "n1"}),
		},
		{
			name:   "drop",
			before: base,
			after:  withoutRows(base, "c"),
			parsed: oneAlter(parsedSpec{Op: "drop", OldName: "c"}),
		},
		{
			name:   "pri implied not null",
			before: base,
			after:  withRows(base, colRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "NO", ColumnKey: "PRI"}),
			parsed: oneAlter(parsedSpec{Op: "add", Cols: []parsedCol{{Name: "n1", TypeStr: "int", Precision: -1, Scale: -1}}}),
		},
		{
			name:   "decimal params",
			before: base,
			after: withRows(base, colRow{
				Name: "n1", Ordinal: 4, ColumnType: "decimal(10,2)", IsNullable: "YES",
				NumPrec: int64Ptr(10), NumScale: int64Ptr(2),
			}),
			parsed: oneAlter(parsedSpec{Op: "add", Cols: []parsedCol{{Name: "n1", TypeStr: "decimal(10,2)", Precision: 10, Scale: 2}}}),
		},
		{
			name:   "after last no shift",
			before: base,
			after:  withRows(base, colRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "YES"}),
			parsed: oneAlter(parsedSpec{Op: "add", HasPosition: true, Cols: []parsedCol{{Name: "n1", TypeStr: "int", Precision: -1, Scale: -1}}}),
		},
		{
			name:   "mariadb json alias remains signal",
			before: base,
			after:  withRows(base, colRow{Name: "n1", Ordinal: 4, ColumnType: "longtext", IsNullable: "YES"}),
			parsed: oneAlter(parsedSpec{Op: "add", Cols: []parsedCol{{Name: "n1", TypeStr: "json", Precision: -1, Scale: -1}}}),
			want:   []string{"e2e-col-attr"},
		},
		{
			name:   "benign classification changed schema",
			before: base,
			after:  withRows(base, colRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "YES"}),
			parsed: parsedStmts{},
			want:   []string{"e2e-missed-column-effect"},
		},
		{
			name:   "position missed",
			before: base,
			after: snapshot{
				"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
				"n1": {Name: "n1", Ordinal: 2, ColumnType: "int", IsNullable: "YES"},
				"c":  {Name: "c", Ordinal: 3, ColumnType: "int", IsNullable: "YES"},
				"d":  {Name: "d", Ordinal: 4, ColumnType: "int", IsNullable: "YES"},
			},
			parsed: oneAlter(parsedSpec{Op: "add", Cols: []parsedCol{{Name: "n1", TypeStr: "int", Precision: -1, Scale: -1}}}),
			want:   []string{"e2e-position-missed"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := e2echeck.CompareSemantics(e2echeck.SemanticInput{
				Before: tc.before,
				After:  tc.after,
				Actual: diffSnapshots(tc.before, tc.after),
			}, tc.parsed)
			got := findingClasses(findings)
			if !sameStrings(got, tc.want) {
				t.Fatalf("classes = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCompareSemanticsRenameTable(t *testing.T) {
	exp := caseExpect{
		BeforeTables: map[string]bool{"fixture": true},
		AfterTables:  map[string]bool{"fixture_r": true},
	}
	parsed := parsedStmts{Stmts: []parsedStmt{{Kind: "rename_table", Pairs: []parsedPair{{OldTable: "fixture", NewTable: "fixture_r"}}}}}
	actual := delta{Renamed: inferRenames(exp.BeforeTables, exp.AfterTables)}
	if got := e2echeck.CompareSemantics(e2echeck.SemanticInput{Actual: actual}, parsed); len(got) != 0 {
		t.Fatalf("rename findings = %v, want none", got)
	}
}

func TestSQLModeRelevantFromReadback(t *testing.T) {
	tests := map[string]uint64{
		"":            0,
		"ANSI_QUOTES": sqlModeANSIQuotes,
		"REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES": sqlModeRealAsFloat | sqlModeANSIQuotes,
		"ANSI_QUOTES,NO_BACKSLASH_ESCAPES":          sqlModeANSIQuotes | sqlModeNoBackslashEscapes,
		"ORACLE,MSSQL,NO_BACKSLASH_ESCAPES":         sqlModeOracle | sqlModeMSSQL | sqlModeNoBackslashEscapes,
	}
	for input, want := range tests {
		if got := relevantFromReadback(input); got != want {
			t.Fatalf("relevantFromReadback(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestExpectQueueAndMarkerDesync(t *testing.T) {
	q := newExpectQueue()
	q.Push(caseExpect{Kind: expectMarker, Submitted: "m1"})
	q.Push(caseExpect{Kind: expectMarker, Submitted: "m2"})
	first, err := q.PopWait(t.Context(), 0)
	if err != nil || first.Submitted != "m1" {
		t.Fatalf("first pop = %#v, %v", first, err)
	}
	second, err := q.PopWait(t.Context(), 0)
	if err != nil || second.Submitted != "m2" {
		t.Fatalf("second pop = %#v, %v", second, err)
	}

	errs := make(chan error, 1)
	m := &matcher{ec: engineConfig{Name: EngineMySQL}, errs: errs}
	m.handleQueryEvent(t.Context(), &replication.BinlogEvent{}, &replication.QueryEvent{Schema: []byte("fuzz_w1"), Query: []byte("wrong")}, caseExpect{
		Kind:      expectMarker,
		CaseID:    "case",
		Submitted: "right",
	})
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("nil desync error")
		}
	default:
		t.Fatal("expected desync error")
	}
}

func oneAlter(specs ...parsedSpec) parsedStmts {
	return parsedStmts{Stmts: []parsedStmt{{Kind: "alter_table", Table: fixtureTable, Specs: specs}}}
}

func withRows(in snapshot, rows ...colRow) snapshot {
	out := cloneSnapshot(in)
	for _, row := range rows {
		out[row.Name] = row
	}
	return out
}

func withReplace(in snapshot, rows ...colRow) snapshot {
	return withRows(in, rows...)
}

func withoutRows(in snapshot, names ...string) snapshot {
	out := cloneSnapshot(in)
	for _, name := range names {
		delete(out, name)
	}
	return out
}

func cloneSnapshot(in snapshot) snapshot {
	out := make(snapshot, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func findingClasses(in []semanticFinding) []string {
	out := make([]string, len(in))
	for i, f := range in {
		out[i] = f.Class
	}
	return out
}

func int64Ptr(v int64) *int64 {
	return &v
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
	}
	for _, n := range counts {
		if n != 0 {
			return false
		}
	}
	return true
}
