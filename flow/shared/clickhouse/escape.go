package clickhouse

import "strings"

const mustEscape = "\t\n`'\\"

func EscapeStr(value string) string {
	var result strings.Builder
	for _, c := range value {
		if strings.ContainsRune(mustEscape, c) {
			result.WriteRune('\\')
		}
		result.WriteRune(c)
	}

	return result.String()
}

func QuoteLiteral(value string) string {
	return "'" + EscapeStr(value) + "'"
}

func QuoteIdentifier(value string) string {
	return "`" + EscapeStr(value) + "`"
}
