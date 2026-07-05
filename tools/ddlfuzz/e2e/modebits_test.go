package e2e

import (
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/gen"
)

// The readback-derived relevant bitmask is passed to gen.Profile.Mode as-is,
// so the e2echeck and gen bit layouts must stay identical.
func TestRelevantModeBitsMatchGen(t *testing.T) {
	pairs := []struct {
		name   string
		e2eBit uint64
		genBit uint64
	}{
		{"ANSI_QUOTES", sqlModeANSIQuotes, gen.ModeANSIQuotes},
		{"ORACLE", sqlModeOracle, gen.ModeOracle},
		{"MSSQL", sqlModeMSSQL, gen.ModeMSSQL},
		{"NO_BACKSLASH_ESCAPES", sqlModeNoBackslashEscapes, gen.ModeNoBackslashEscapes},
	}
	for _, p := range pairs {
		if p.e2eBit != p.genBit {
			t.Errorf("%s: e2echeck bit %#x != gen bit %#x", p.name, p.e2eBit, p.genBit)
		}
	}
}

func TestIncompatibleSQLModeToken(t *testing.T) {
	cases := []struct {
		engine string
		mode   string
		want   string
	}{
		{EngineMySQL, "", ""},
		{EngineMySQL, "ANSI_QUOTES", ""},
		{EngineMySQL, "ANSI_QUOTES,NO_BACKSLASH_ESCAPES", ""},
		{EngineMySQL, "ANSI", ""},
		{EngineMySQL, "ORACLE", "ORACLE"},
		{EngineMySQL, "ORACLE,NO_BACKSLASH_ESCAPES", "ORACLE"},
		{EngineMySQL, "MSSQL", "MSSQL"},
		{EngineMySQL, "MSSQL,NO_BACKSLASH_ESCAPES", "MSSQL"},
		{EngineMySQL, "NO_BACKSLASH_ESCAPES,MSSQL", "MSSQL"},
		{EngineMySQL, " oracle , ansi_quotes ", "ORACLE"},
		{"MySQL", "ORACLE", "ORACLE"},
		{EngineMariaDB, "ORACLE", ""},
		{EngineMariaDB, "MSSQL,NO_BACKSLASH_ESCAPES", ""},
	}
	for _, tc := range cases {
		if got := incompatibleSQLModeToken(tc.engine, tc.mode); got != tc.want {
			t.Errorf("incompatibleSQLModeToken(%q, %q) = %q, want %q", tc.engine, tc.mode, got, tc.want)
		}
	}
}
