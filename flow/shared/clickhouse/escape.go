package clickhouse

import "strings"

func mustEscape(char byte) bool {
	return char == '\'' || char == '`' || char == '\\' || char == '?' || char == '\t' || char == '\n'
}

// escaped size only needs to iterate on bytes, ASCII will never appear within multibyte utf8 characters
func escapeSize(value string) int {
	size := len(value)
	for idx := range len(value) {
		if mustEscape(value[idx]) {
			size += 1
		}
	}
	return size
}

func escape(result *strings.Builder, value string) {
	for idx := range len(value) {
		if mustEscape(value[idx]) {
			result.WriteByte('\\')
		}
		result.WriteByte(value[idx])
	}
}

func EscapeStr(value string) string {
	var result strings.Builder
	result.Grow(escapeSize(value))
	escape(&result, value)
	return result.String()
}

func QuoteLiteral(value string) string {
	var result strings.Builder
	result.Grow(escapeSize(value) + 2)
	result.WriteByte('\'')
	escape(&result, value)
	result.WriteByte('\'')
	return result.String()
}

func QuoteIdentifier(value string) string {
	var result strings.Builder
	result.Grow(escapeSize(value) + 2)
	result.WriteByte('`')
	escape(&result, value)
	result.WriteByte('`')
	return result.String()
}
