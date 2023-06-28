package utils

import "fmt"

func QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}
