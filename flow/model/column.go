package model

type ColumnInformation struct {
	// This is a mapping from column name to column type
	// Example: "name" -> "VARCHAR"
	ColumnMap map[string]string
	Columns   []string // List of column names
}
