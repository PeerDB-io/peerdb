package shared

import (
	"strings"
	"unicode"
)

// start with [A-Za-z_]
// subsequently contain only [A-Za-z0-9_]
func SanitizeColumnNameForAvro(colName string) string {
	if colName == "" {
		return colName
	}

	var result strings.Builder
	result.Grow(len(colName))

	if !unicode.IsLetter(rune(colName[0])) && colName[0] != '_' {
		result.WriteByte('_')
	}
	result.WriteByte(colName[0])

	for _, char := range colName[1:] {
		if unicode.IsLetter(char) || unicode.IsDigit(char) || char == '_' {
			result.WriteRune(char)
		} else {
			result.WriteByte('_')
		}
	}

	return result.String()
}
