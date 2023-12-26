package utils

import (
	"fmt"
	"strings"
)

func QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}

// SchemaTable is a table in a schema.
type SchemaTable struct {
	Schema string
	Table  string
}

func (t *SchemaTable) String() string {
	return fmt.Sprintf(`"%s"."%s"`, t.Schema, t.Table)
}

// ParseSchemaTable parses a table name into schema and table name.
func ParseSchemaTable(tableName string) (*SchemaTable, error) {
	schema, table, hasDot := strings.Cut(tableName, ".")
	if !hasDot || strings.ContainsRune(table, '.') {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	return &SchemaTable{schema, table}, nil
}
