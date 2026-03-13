package common

import (
	"fmt"
	"regexp"
	"strings"
)

var loneDotRe = regexp.MustCompile(`[^.]\.[^.]`)

type QualifiedTable struct {
	Namespace string // schema in PG, database in MySQL/Mongo
	Table     string // table or collection
}

func (t *QualifiedTable) Deparse() string {
	return fmt.Sprintf("%s.%s", strings.ReplaceAll(t.Namespace, ".", ".."), strings.ReplaceAll(t.Table, ".", ".."))
}

func (t *QualifiedTable) String() string {
	return fmt.Sprintf("%s.%s", QuoteIdentifier(t.Namespace), QuoteIdentifier(t.Table))
}

func (t *QualifiedTable) MySQL() string {
	return fmt.Sprintf("%s.%s", QuoteMySQLIdentifier(t.Namespace), QuoteMySQLIdentifier(t.Table))
}

// ParseTableIdentifier parses a table name into namespace and table name.
func ParseTableIdentifier(tableIdentifier string) (*QualifiedTable, error) {
	if !strings.Contains(tableIdentifier, "..") {
		// fast path
		ns, table, hasDot := strings.Cut(tableIdentifier, ".")
		if !hasDot || ns == "" || table == "" || strings.ContainsRune(table, '.') {
			return nil, fmt.Errorf("invalid table name: %s", tableIdentifier)
		}
		return &QualifiedTable{ns, table}, nil
	} else {
		splits := loneDotRe.Split(tableIdentifier, 3)
		if len(splits) != 2 {
			return nil, fmt.Errorf("invalid table name: %s", tableIdentifier)
		}
		return &QualifiedTable{
			Namespace: strings.ReplaceAll(splits[0], "..", "."),
			Table:     strings.ReplaceAll(splits[1], "..", "."),
		}, nil
	}
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//	tblname := "my_table"
//	data := "my_data"
//	quoted := common.QuoteIdentifier(tblname)
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

func QuoteMySQLIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
