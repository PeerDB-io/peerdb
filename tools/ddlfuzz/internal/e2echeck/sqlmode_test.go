package e2echeck

import "testing"

func TestExpectedEventSQLModeRelevant(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		session   uint64
		isMariaDB bool
		want      uint64
	}{
		{
			name:      "plain statement keeps session mode",
			sql:       "ALTER TABLE fixture ADD COLUMN n1 INT",
			session:   SQLModeRealAsFloat | SQLModeNoBackslashEscapes,
			isMariaDB: true,
			want:      SQLModeRealAsFloat | SQLModeNoBackslashEscapes,
		},
		{
			name:      "mysql ignores set statement adjustment",
			sql:       "SET STATEMENT sql_mode=CONCAT(@@sql_mode, ',ANSI_QUOTES') FOR ALTER TABLE fixture ADD COLUMN n1 INT",
			session:   SQLModeNoBackslashEscapes,
			isMariaDB: false,
			want:      SQLModeNoBackslashEscapes,
		},
		{
			name:      "mariadb concat appends relevant mode",
			sql:       "SET STATEMENT sql_mode=CONCAT(@@sql_mode, ',ANSI_QUOTES'), max_statement_time=60 FOR ALTER TABLE fixture ADD COLUMN n1 INT",
			session:   SQLModeNoBackslashEscapes,
			isMariaDB: true,
			want:      SQLModeNoBackslashEscapes | SQLModeANSIQuotes,
		},
		{
			name:      "for inside string is skipped",
			sql:       "SET STATEMENT lc_messages='en FOR us', sql_mode=CONCAT(@@session.sql_mode, ',MSSQL') FOR ALTER TABLE fixture ADD COLUMN n1 INT",
			session:   SQLModeNoBackslashEscapes,
			isMariaDB: true,
			want:      SQLModeNoBackslashEscapes | SQLModeMSSQL,
		},
		{
			name:      "literal replaces session mode",
			sql:       "SET STATEMENT sql_mode='ANSI_QUOTES' FOR ALTER TABLE fixture ADD COLUMN n1 INT",
			session:   SQLModeNoBackslashEscapes,
			isMariaDB: true,
			want:      SQLModeANSIQuotes,
		},
		{
			name:      "concat executable comment and charset literal",
			sql:       "SET STATEMENT\nsql_mode=CONCAT(/*! @@sql_mode */,_utf8mb4',ANSI_QUOTES') FOR ALTER TABLE fixture ADD COLUMN n1 INT",
			session:   0,
			isMariaDB: true,
			want:      SQLModeANSIQuotes,
		},
		{
			name:      "concat executable comment comma separator",
			sql:       "SET STATEMENT sql_mode=CONCAT(@@sql_mode/*!50001 , */',ANSI_QUOTES') FOR ALTER TABLE fixture ADD n1 INT",
			session:   0,
			isMariaDB: true,
			want:      SQLModeANSIQuotes,
		},
		{
			name:      "concat national charset literal",
			sql:       "SET STATEMENT sql_mode=CONCAT(@@sql_mode,N',ANSI_QUOTES') FOR ALTER TABLE fixture ADD n1 INT",
			session:   0,
			isMariaDB: true,
			want:      SQLModeANSIQuotes,
		},
		{
			name:      "unsupported expression falls back to session mode",
			sql:       "SET STATEMENT sql_mode=REPLACE(@@sql_mode, 'ANSI_QUOTES', '') FOR ALTER TABLE fixture ADD COLUMN n1 INT",
			session:   SQLModeNoBackslashEscapes | SQLModeANSIQuotes,
			isMariaDB: true,
			want:      SQLModeNoBackslashEscapes | SQLModeANSIQuotes,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExpectedEventSQLModeRelevant(tc.sql, tc.session, tc.isMariaDB)
			if got != tc.want {
				t.Fatalf("ExpectedEventSQLModeRelevant = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestSQLModeNames(t *testing.T) {
	tests := []struct {
		name string
		mode uint64
		want string
	}{
		{name: "zero", mode: 0, want: ""},
		{name: "real as float", mode: SQLModeRealAsFloat, want: "REAL_AS_FLOAT"},
		{name: "ansi quotes", mode: SQLModeANSIQuotes, want: "ANSI_QUOTES"},
		{name: "oracle", mode: SQLModeOracle, want: "ORACLE"},
		{name: "mssql", mode: SQLModeMSSQL, want: "MSSQL"},
		{name: "no backslash escapes", mode: SQLModeNoBackslashEscapes, want: "NO_BACKSLASH_ESCAPES"},
		{
			name: "canonical order",
			mode: SQLModeNoBackslashEscapes | SQLModeMSSQL | SQLModeANSIQuotes | SQLModeOracle | SQLModeRealAsFloat,
			want: "REAL_AS_FLOAT,ANSI_QUOTES,ORACLE,MSSQL,NO_BACKSLASH_ESCAPES",
		},
		{name: "irrelevant bits masked", mode: SQLModeANSIQuotes | (1 << 63), want: "ANSI_QUOTES"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SQLModeNames(tc.mode)
			if got != tc.want {
				t.Fatalf("SQLModeNames(%d) = %q, want %q", tc.mode, got, tc.want)
			}
			if roundTrip := sqlModeNamesRelevant(got); roundTrip != tc.mode&RelevantSQLModeMask {
				t.Fatalf("sqlModeNamesRelevant(SQLModeNames(%d)) = %d, want %d", tc.mode, roundTrip, tc.mode&RelevantSQLModeMask)
			}
		})
	}
}
