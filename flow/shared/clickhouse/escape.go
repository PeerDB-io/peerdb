package clickhouse

const BS = '\\'
const mustEscape = "\t\n`'\\"

func EscapeStr(value string) string {
	result := ""

	for _, c := range value {
		if containsRune(mustEscape, c) {
			result += string(BS)
		}
		result += string(c)
	}

	return result
}

func containsRune(s string, r rune) bool {
	for _, c := range s {
		if c == r {
			return true
		}
	}
	return false
}
