package e2e

import "testing"

func TestWorkerPaletteEntryQueueModeResolution(t *testing.T) {
	empty := ""
	oracle := "ORACLE"
	tests := []struct {
		name string
		item *queueItem
		want string
	}{
		{
			name: "session sql mode empty is authoritative",
			item: &queueItem{SessionSQLMode: &empty, SQLModeName: "ORACLE", SQLMode: sqlModeANSIQuotes},
			want: "",
		},
		{
			name: "session sql mode value is authoritative",
			item: &queueItem{SessionSQLMode: &oracle, SQLModeName: "ANSI_QUOTES", SQLMode: sqlModeNoBackslashEscapes},
			want: "ORACLE",
		},
		{
			name: "legacy name fallback",
			item: &queueItem{SQLModeName: "NO_BACKSLASH_ESCAPES", SQLMode: sqlModeANSIQuotes},
			want: "NO_BACKSLASH_ESCAPES",
		},
		{
			name: "legacy bits fallback",
			item: &queueItem{SQLMode: sqlModeANSIQuotes | sqlModeNoBackslashEscapes},
			want: "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
		},
		{
			name: "legacy zero bits fallback",
			item: &queueItem{},
			want: "",
		},
	}
	ws := &workerState{rt: &engineRuntime{ec: engineConfig{Name: EngineMySQL}}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, seq := range []uint64{1, 7, 99} {
				if got := ws.paletteEntry(seq, tc.item); got != tc.want {
					t.Fatalf("paletteEntry(%d, item) = %q, want %q", seq, got, tc.want)
				}
				if got := tc.item.sessionMode(); got != tc.want {
					t.Fatalf("sessionMode() = %q, want %q", got, tc.want)
				}
			}
		})
	}
}

func TestWorkerPaletteEntryFuzzPalette(t *testing.T) {
	mysql := &workerState{rt: &engineRuntime{ec: engineConfig{Name: EngineMySQL}}}
	if got, want := mysql.paletteEntry(4, nil), "ANSI_QUOTES"; got != want {
		t.Fatalf("mysql paletteEntry = %q, want %q", got, want)
	}
	if got, want := mysql.paletteEntry(uint64(len(mysqlSQLModes)+1), nil), ""; got != want {
		t.Fatalf("mysql paletteEntry wrap = %q, want %q", got, want)
	}

	maria := &workerState{rt: &engineRuntime{ec: engineConfig{Name: EngineMariaDB, IsMariaDB: true}}}
	if got, want := maria.paletteEntry(8, nil), "ORACLE"; got != want {
		t.Fatalf("mariadb paletteEntry = %q, want %q", got, want)
	}
	if got, want := maria.paletteEntry(uint64(len(mariaSQLModes)+9), nil), "MSSQL"; got != want {
		t.Fatalf("mariadb paletteEntry wrap = %q, want %q", got, want)
	}
}
