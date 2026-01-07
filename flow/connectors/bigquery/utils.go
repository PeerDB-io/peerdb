package connbigquery

import (
	"fmt"
	"strings"
)

func quoteIdentifier(sourceTableIdentifier string) string {
	parts := strings.Split(sourceTableIdentifier, ".")
	for i, part := range parts {
		// Escape backticks by doubling them
		escapedPart := strings.ReplaceAll(part, "`", "``")
		parts[i] = fmt.Sprintf("`%s`", escapedPart)
	}
	return strings.Join(parts, ".")
}
