package connbigquery

import (
	"strings"
)

func quotedIdentifier(value string) string {
	var result strings.Builder
	result.WriteByte('`')
	for i := 0; i < len(value); i++ {
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
