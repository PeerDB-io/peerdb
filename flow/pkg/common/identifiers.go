package common

import (
	"fmt"
	"strings"
)

type QualifiedTable struct {
	Namespace string // schema in PG, database in MySQL/Mongo
	Table     string // table or collection
}

func (t *QualifiedTable) String() string {
	return fmt.Sprintf("%s.%s", QuoteIdentifier(t.Namespace), QuoteIdentifier(t.Table))
}

func (t *QualifiedTable) MySQL() string {
	return fmt.Sprintf("`%s`.`%s`", t.Namespace, t.Table)
}

// ParseTableIdentifier parses a table name into namespace and table name.
func ParseTableIdentifier(tableIdentifier string) (*QualifiedTable, error) {
	ns, table, hasDot := strings.Cut(tableIdentifier, ".")
	if !hasDot || ns == "" || table == "" || strings.ContainsRune(table, '.') {
		return nil, fmt.Errorf("invalid table name: %s", tableIdentifier)
	}

	return &QualifiedTable{ns, table}, nil
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//	tblname := "my_table"
//	data := "my_data"
//	quoted := pq.QuoteIdentifier(tblname)
//	err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case-sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
