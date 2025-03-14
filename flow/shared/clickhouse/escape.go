package clickhouse

import "strings"

const BS = '\\'
const mustEscape = "\t\n`'\\"

func EscapeStr(value string) string {
	result := ""

	for _, c := range value {
		if strings.ContainsRune(mustEscape, c) {
			result += string(BS)
		}
		result += string(c)
	}

	return result
}
