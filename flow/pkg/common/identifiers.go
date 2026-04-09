package common

import (
	"fmt"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
)

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

func (t QualifiedTable) MsSql() string {
	return fmt.Sprintf("%s.%s", QuoteMsSqlIdentifier(t.Namespace), QuoteMsSqlIdentifier(t.Table))
}

// ParseTableIdentifier parses a table name into namespace and table name.
// Dots within namespace or table are escaped as ".." by Deparse.
// A lone single dot (not part of "..") separates namespace from table.
func ParseTableIdentifier(tableIdentifier string) (*QualifiedTable, error) {
	// Walk the string looking for the separator: a dot not adjacent to another dot.
	// Escaped dots ("..") are skipped in pairs.
	sep := -1
	for i := 0; i < len(tableIdentifier); i++ {
		if tableIdentifier[i] != '.' {
			continue
		}
		if i+1 < len(tableIdentifier) && tableIdentifier[i+1] == '.' {
			i++ // skip escaped dot pair
			continue
		}
		if sep != -1 {
			return nil, fmt.Errorf("invalid table name: %s", tableIdentifier)
		}
		sep = i
	}
	if sep <= 0 || sep >= len(tableIdentifier)-1 {
		return nil, fmt.Errorf("invalid table name: %s", tableIdentifier)
	}
	return &QualifiedTable{
		Namespace: strings.ReplaceAll(tableIdentifier[:sep], "..", "."),
		Table:     strings.ReplaceAll(tableIdentifier[sep+1:], "..", "."),
	}, nil
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

func QuoteMsSqlIdentifier(name string) string {
	return mssql.TSQLQuoter{}.ID(name)
}
