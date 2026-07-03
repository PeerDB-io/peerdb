package e2echeck

import "strings"

const sqlBoundaryWhitespace = " \t\n\r\f\v"

func EquivalentQueryText(a, b string) bool {
	return strings.Trim(a, sqlBoundaryWhitespace) == strings.Trim(b, sqlBoundaryWhitespace)
}
