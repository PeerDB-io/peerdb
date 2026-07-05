package e2e

import (
	"strings"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/e2echeck"
)

const (
	sqlModeANSIQuotes         = e2echeck.SQLModeANSIQuotes
	sqlModeOracle             = e2echeck.SQLModeOracle
	sqlModeMSSQL              = e2echeck.SQLModeMSSQL
	sqlModeNoBackslashEscapes = e2echeck.SQLModeNoBackslashEscapes
	relevantMask              = e2echeck.RelevantSQLModeMask
)

var relevantBits = map[string]uint64{
	"ANSI_QUOTES":          sqlModeANSIQuotes,
	"ORACLE":               sqlModeOracle,
	"MSSQL":                sqlModeMSSQL,
	"NO_BACKSLASH_ESCAPES": sqlModeNoBackslashEscapes,
}

var mysqlSQLModes = []string{
	"",
	"",
	"",
	"ANSI_QUOTES",
	"NO_BACKSLASH_ESCAPES",
	"ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
	"ANSI",
}

var mariaSQLModes = []string{
	"",
	"",
	"",
	"ANSI_QUOTES",
	"NO_BACKSLASH_ESCAPES",
	"ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
	"ANSI",
	"ORACLE",
	"MSSQL",
	"MSSQL,NO_BACKSLASH_ESCAPES",
}

func sqlModePalette(isMariaDB bool) []string {
	if isMariaDB {
		return mariaSQLModes
	}
	return mysqlSQLModes
}

func incompatibleSQLModeToken(engine, mode string) string {
	if !strings.EqualFold(engine, EngineMySQL) {
		return ""
	}
	for _, raw := range strings.Split(mode, ",") {
		name := strings.ToUpper(strings.TrimSpace(raw))
		switch name {
		case "ORACLE", "MSSQL":
			return name
		}
	}
	return ""
}

func relevantFromReadback(readback string) uint64 {
	var out uint64
	for _, raw := range strings.Split(readback, ",") {
		name := strings.ToUpper(strings.TrimSpace(raw))
		if bit, ok := relevantBits[name]; ok {
			out |= bit
		}
	}
	return out & relevantMask
}
