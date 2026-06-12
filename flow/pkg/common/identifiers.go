package common

import (
	"fmt"
	"strings"
)

type QualifiedTable struct {
	Namespace string // schema in PG, database in MySQL/Mongo
	Table     string // table or collection
}

func (t QualifiedTable) String() string {
	return fmt.Sprintf("%s.%s", QuoteIdentifier(t.Namespace), QuoteIdentifier(t.Table))
}

func (t QualifiedTable) MySQL() string {
	return fmt.Sprintf("%s.%s", QuoteMySQLIdentifier(t.Namespace), QuoteMySQLIdentifier(t.Table))
}

// LegacyDotted renders the identifier in the unquoted dotted wire format used before
// QualifiedTable was introduced. Only for surfaces where that format is persisted or
// user-exposed and must stay stable: _peerdb_destination_table_name values in raw
// tables, the Lua script API, child workflow IDs and API display fields. Ambiguous when
// a component contains a dot (mirror validation rejects colliding pairs); never use it
// for logs or new functionality.
func (t QualifiedTable) LegacyDotted() string {
	if t.Namespace == "" {
		return t.Table
	}
	return t.Namespace + "." + t.Table
}

// NormalizeTableIdentifier converts a legacy dotted identifier into a QualifiedTable
// with the pre-QualifiedTable first-dot-split semantics; identifiers without a dot are
// table-only. Pure and total so it is safe inside Temporal workflow code.
func NormalizeTableIdentifier(identifier string) QualifiedTable {
	namespace, table, hasDot := strings.Cut(identifier, ".")
	if !hasDot {
		return QualifiedTable{Table: identifier}
	}
	return QualifiedTable{Namespace: namespace, Table: table}
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
