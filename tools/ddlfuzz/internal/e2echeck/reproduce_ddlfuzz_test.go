//go:build ddlfuzz

package e2echeck

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestReproduceByClass(t *testing.T) {
	base := Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
		"c":  {Name: "c", Ordinal: 2, ColumnType: "int", IsNullable: "YES"},
		"d":  {Name: "d", Ordinal: 3, ColumnType: "int", IsNullable: "YES"},
	}
	validStatus := statusVarsHex(0)
	ansiStatus := statusVarsHex(SQLModeANSIQuotes)
	expectedZero := uint64(0)
	expectedANSI := SQLModeANSIQuotes

	tests := []struct {
		name      string
		in        Input
		wantClass string
	}{
		{
			name: "statusvar walk diverges",
			in: baseInput(ClassStatusVarWalk,
				"ALTER TABLE t ADD COLUMN n1 INT",
				"ALTER TABLE t ADD COLUMN n1 INT",
				"02"),
			wantClass: ClassStatusVarWalk,
		},
		{
			name: "statusvar walk reconciled",
			in: baseInput(ClassStatusVarWalk,
				"ALTER TABLE t ADD COLUMN n1 INT",
				"ALTER TABLE t ADD COLUMN n1 INT",
				validStatus),
		},
		{
			name: "sqlmode mismatch diverges",
			in: func() Input {
				in := baseInput(ClassSQLModeMismatch,
					"ALTER TABLE t ADD COLUMN n1 INT",
					"ALTER TABLE t ADD COLUMN n1 INT",
					ansiStatus)
				in.SQLMode = SQLModeANSIQuotes
				in.ExpectedRelevant = &expectedZero
				return in
			}(),
			wantClass: ClassSQLModeMismatch,
		},
		{
			name: "sqlmode mismatch reconciled",
			in: func() Input {
				in := baseInput(ClassSQLModeMismatch,
					"ALTER TABLE t ADD COLUMN n1 INT",
					"ALTER TABLE t ADD COLUMN n1 INT",
					ansiStatus)
				in.SQLMode = SQLModeANSIQuotes
				in.ExpectedRelevant = &expectedANSI
				return in
			}(),
		},
		{
			name: "plumbing sig diverges",
			in: func() Input {
				in := baseInput(ClassPlumbingSig,
					`ALTER TABLE "t" ADD COLUMN "n1" INT`,
					`ALTER TABLE "t" ADD COLUMN "n1" INT`,
					ansiStatus)
				in.SQLMode = 0
				return in
			}(),
			wantClass: ClassPlumbingSig,
		},
		{
			name: "plumbing sig reconciled",
			in: baseInput(ClassPlumbingSig,
				"ALTER TABLE t ADD COLUMN n1 INT",
				"ALTER TABLE t ADD COLUMN n1 INT",
				validStatus),
		},
		{
			name: "query rewrite diverges",
			in: baseInput(ClassQueryRewrite,
				"ALTER TABLE t ADD COLUMN n1 INT",
				"ALTER TABLE t ADD COLUMN n1 BIGINT",
				validStatus),
			wantClass: ClassQueryRewrite,
		},
		{
			name: "query rewrite reconciled",
			in: baseInput(ClassQueryRewrite,
				"ALTER TABLE t ADD COLUMN n1 INT",
				"ALTER TABLE t ADD COLUMN n1 INT",
				validStatus),
		},
		{
			name: "query rewrite boundary whitespace reconciled",
			in: baseInput(ClassQueryRewrite,
				" \tALTER TABLE t ADD COLUMN n1 INT \r\n",
				"ALTER TABLE t ADD COLUMN n1 INT",
				validStatus),
		},
		{
			name: "mariadb reversed skipped comment rewrite reconciled",
			in: func() Input {
				in := baseInput(ClassQueryRewrite,
					"ALTER TABLE fixture ADD n1 INT /*!!11050 NOT NULL*/",
					"ALTER TABLE fixture ADD n1 INT /*  11050 NOT NULL*/",
					validStatus)
				in.Engine = "mariadb"
				in.IsMariaDB = true
				return in
			}(),
		},
		{
			name: "mysql reversed skipped comment rewrite still diverges",
			in: baseInput(ClassQueryRewrite,
				"ALTER TABLE fixture ADD n1 INT /*!!11050 NOT NULL*/",
				"ALTER TABLE fixture ADD n1 INT /*  11050 NOT NULL*/",
				validStatus),
			wantClass: ClassQueryRewrite,
		},
		{
			name: "panic class reconciled",
			in: baseInput(ClassPanic,
				"ALTER TABLE t ADD COLUMN n1 INT",
				"ALTER TABLE t ADD COLUMN n1 INT",
				validStatus),
		},
		{
			name: "parse error live accept diverges",
			in: withSnapshots(baseInput(ClassParseErrorLiveAccept,
				"ALTER TABLE t ADD COLUMN",
				"ALTER TABLE t ADD COLUMN",
				validStatus), base, base),
			wantClass: ClassParseErrorLiveAccept,
		},
		{
			name: "parse error live accept reconciled",
			in: withSnapshots(baseInput(ClassParseErrorLiveAccept,
				"ALTER TABLE t ADD COLUMN n1 INT",
				"ALTER TABLE t ADD COLUMN n1 INT",
				validStatus), base, withRows(base, ColRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "YES"})),
		},
		{
			name: "missed column effect diverges",
			in: func() Input {
				after := withRows(base, ColRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "YES"})
				return withSnapshots(baseInput(ClassMissedColumnEffect, "SELECT 1", "SELECT 1", validStatus), base, after)
			}(),
			wantClass: ClassMissedColumnEffect,
		},
		{
			name: "missed column effect reconciled",
			in: func() Input {
				after := withRows(base, ColRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "YES"})
				return withSnapshots(baseInput(ClassMissedColumnEffect,
					"ALTER TABLE t ADD COLUMN n1 INT",
					"ALTER TABLE t ADD COLUMN n1 INT",
					validStatus), base, after)
			}(),
		},
		{
			name: "column attr diverges",
			in: func() Input {
				after := withRows(base, ColRow{Name: "n1", Ordinal: 4, ColumnType: "bigint", IsNullable: "YES"})
				return withSnapshots(baseInput(ClassColumnAttr,
					"ALTER TABLE t ADD COLUMN n1 INT",
					"ALTER TABLE t ADD COLUMN n1 INT",
					validStatus), base, after)
			}(),
			wantClass: ClassColumnAttr,
		},
		{
			name: "pri not null reconciled",
			in: func() Input {
				after := withRows(base, ColRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "NO", ColumnKey: "PRI"})
				return withSnapshots(baseInput(ClassColumnAttr,
					"ALTER TABLE t ADD COLUMN n1 INT",
					"ALTER TABLE t ADD COLUMN n1 INT",
					validStatus), base, after)
			}(),
		},
		{
			// The MariaDB json→longtext alias reconciles because the parser
			// itself emits longtext for JSON on MariaDB (ddl_parser.go); the
			// compare code deliberately has no alias special case
			// (30-e2e-lane.md §Reconciliations), so a parser regression here
			// would diverge.
			name: "mariadb json alias reconciled by parser",
			in: func() Input {
				after := withRows(base, ColRow{Name: "j", Ordinal: 4, ColumnType: "longtext", IsNullable: "YES"})
				in := withSnapshots(baseInput(ClassColumnAttr,
					"ALTER TABLE t ADD COLUMN j JSON",
					"ALTER TABLE t ADD COLUMN j JSON",
					validStatus), base, after)
				in.Engine = "mariadb"
				in.IsMariaDB = true
				return in
			}(),
		},
		{
			name: "decimal default reconciled",
			in: func() Input {
				after := withRows(base, ColRow{
					Name: "n1", Ordinal: 4, ColumnType: "decimal(10,0)", IsNullable: "YES",
					NumPrec: int64Ptr(10), NumScale: int64Ptr(0),
				})
				return withSnapshots(baseInput(ClassColumnAttr,
					"ALTER TABLE t ADD COLUMN n1 DECIMAL",
					"ALTER TABLE t ADD COLUMN n1 DECIMAL",
					validStatus), base, after)
			}(),
		},
		{
			name: "position missed diverges",
			in: func() Input {
				after := Snapshot{
					"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
					"n1": {Name: "n1", Ordinal: 2, ColumnType: "int", IsNullable: "YES"},
					"c":  {Name: "c", Ordinal: 3, ColumnType: "int", IsNullable: "YES"},
					"d":  {Name: "d", Ordinal: 4, ColumnType: "int", IsNullable: "YES"},
				}
				return withSnapshots(baseInput(ClassPositionMissed,
					"ALTER TABLE t ADD COLUMN n1 INT",
					"ALTER TABLE t ADD COLUMN n1 INT",
					validStatus), base, after)
			}(),
			wantClass: ClassPositionMissed,
		},
		{
			name: "after last no shift reconciled",
			in: func() Input {
				after := withRows(base, ColRow{Name: "n1", Ordinal: 4, ColumnType: "int", IsNullable: "YES"})
				return withSnapshots(baseInput(ClassPositionMissed,
					"ALTER TABLE t ADD COLUMN n1 INT AFTER d",
					"ALTER TABLE t ADD COLUMN n1 INT AFTER d",
					validStatus), base, after)
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Reproduce(tc.in)
			if err != nil {
				t.Fatalf("Reproduce error: %v", err)
			}
			if tc.wantClass == "" {
				if !got.Reconciled {
					t.Fatalf("Reconciled = false, class=%q shape=%q detail=%q", got.Class, got.Shape, got.Detail)
				}
				return
			}
			if got.Reconciled || got.Class != tc.wantClass {
				t.Fatalf("class = %q reconciled=%v, want class %q", got.Class, got.Reconciled, tc.wantClass)
			}
		})
	}
}

func statusVarsHex(mode uint64) string {
	var buf [14]byte
	buf[0] = 0
	buf[5] = 1
	binary.LittleEndian.PutUint64(buf[6:], mode)
	return hex.EncodeToString(buf[:])
}

func baseInput(class, submitted, binlogQuery, statusVarsHex string) Input {
	return Input{
		Engine:        "mysql",
		SQLMode:       0,
		Submitted:     submitted,
		BinlogQuery:   binlogQuery,
		StatusVarsHex: statusVarsHex,
		Before:        Snapshot{},
		After:         Snapshot{},
		Delta:         Delta{},
		Class:         class,
	}
}

func withSnapshots(in Input, before, after Snapshot) Input {
	in.Before = before
	in.After = after
	in.Delta = DiffSnapshots(before, after)
	return in
}

func withRows(in Snapshot, rows ...ColRow) Snapshot {
	out := make(Snapshot, len(in)+len(rows))
	for name, row := range in {
		out[name] = row
	}
	for _, row := range rows {
		out[row.Name] = row
	}
	return out
}

func int64Ptr(v int64) *int64 {
	return &v
}
