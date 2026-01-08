package connbigquery

import (
	"strings"
)

func quotedIdentifier(value string) string {
	var result strings.Builder
	result.WriteByte('`')
	for i := range len(value) {
		ch := value[i]
		if ch == '`' {
			result.WriteString("\\`")
		} else {
			result.WriteByte(ch)
		}
	}
	result.WriteByte('`')
	return result.String()
}
