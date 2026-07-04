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
			session:   SQLModeNoBackslashEscapes,
			isMariaDB: true,
			want:      SQLModeNoBackslashEscapes,
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
