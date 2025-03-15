package clickhouse

import "strings"

const (
	BS         = '\\'
	mustEscape = "\t\n`'\\"
)

func EscapeStr(value string) string {
	var result strings.Builder
	for _, c := range value {
		if strings.ContainsRune(mustEscape, c) {
			result.WriteRune(BS)
		}
		result.WriteRune(c)
	}

	return result.String()
}
