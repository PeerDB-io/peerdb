package utils

import (
	"fmt"
	"strings"
	"unicode"
)

// SchemaTable is a table in a schema.
type SchemaTable struct {
	Schema string
	Table  string
}

func (t *SchemaTable) String() string {
	return fmt.Sprintf(`"%s"."%s"`, t.Schema, t.Table)
}

func (t *SchemaTable) MySQL() string {
	return fmt.Sprintf("`%s`.`%s`", t.Schema, t.Table)
}

// ParseSchemaTable parses a table name into schema and table name.
func ParseSchemaTable(tableName string) (*SchemaTable, error) {
	schema, table, hasDot := strings.Cut(tableName, ".")
	if !hasDot || strings.ContainsRune(table, '.') {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	return &SchemaTable{schema, table}, nil
}

// I think these only work with ASCII?
func IsUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

// I think these only work with ASCII?
func IsLower(s string) bool {
	for _, r := range s {
		if !unicode.IsLower(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func RemoveSpacesTabsNewlines(s string) string {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\t", "")
	s = strings.ReplaceAll(s, "\n", "")
	return s
}
