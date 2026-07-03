package e2e

import "strings"

const (
	sqlModeANSIQuotes         uint64 = 1 << 2
	sqlModeOracle             uint64 = 1 << 9
	sqlModeMSSQL              uint64 = 1 << 10
	sqlModeNoBackslashEscapes uint64 = 1 << 20
	relevantMask                     = sqlModeANSIQuotes | sqlModeOracle | sqlModeMSSQL | sqlModeNoBackslashEscapes
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
