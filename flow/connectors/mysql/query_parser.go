package connmysql

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

func isStatementKeywordByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// leadingKeywords returns up to limit uppercased word tokens from the start of a
// SQL statement, skipping leading/inline whitespace and -- , # and /* */ comments.
// It only has to look at the head of the statement, so it never scans a whole
// procedure body. Used to classify statements that failed to parse, where the AST
// is unavailable and only the raw query text remains.
func leadingKeywords(query string, limit int) []string {
	out := make([]string, 0, limit)
	for i, n := 0, len(query); i < n && len(out) < limit; {
		switch c := query[i]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '/' && i+1 < n && query[i+1] == '*':
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return out
			}
			i += 2 + end + 2
		case (c == '-' && i+1 < n && query[i+1] == '-') || c == '#':
			nl := strings.IndexByte(query[i:], '\n')
			if nl < 0 {
				return out
			}
			i += nl + 1
		case isStatementKeywordByte(c):
			j := i + 1
			for j < n && isStatementKeywordByte(query[j]) {
				j++
			}
			out = append(out, strings.ToUpper(query[i:j]))
			i = j
		default:
			// punctuation/operators (=, (, `, ', @, %, ...) — not part of a keyword
			i++
		}
	}
	return out
}

// objectKeywords are the keywords naming what a CREATE/ALTER/DROP/RENAME statement
// operates on. The first one to appear identifies the object kind; it precedes any
// routine/trigger/view body and any IGNORE/ONLINE/DEFINER/ALGORITHM modifiers, so
// scanning for it never trips over a body that happens to mention TABLE.
var objectKeywords = map[string]struct{}{
	"TABLE": {}, "TABLES": {}, "DATABASE": {}, "SCHEMA": {}, "INDEX": {}, "VIEW": {},
	"PROCEDURE": {}, "FUNCTION": {}, "TRIGGER": {}, "EVENT": {}, "USER": {},
	"TABLESPACE": {}, "SEQUENCE": {}, "SERVER": {}, "ROLE": {}, "LOGFILE": {},
}

func firstObjectKeyword(kw []string) string {
	for _, w := range kw {
		if _, ok := objectKeywords[w]; ok {
			return w
		}
	}
	return ""
}

// indexConstraintKeywords are the keywords that, immediately following ADD or DROP
// in an ALTER TABLE spec, name an index, key, or constraint rather than a column,
// e.g. ADD UNIQUE INDEX, ADD FULLTEXT KEY, ADD PRIMARY KEY, ADD CONSTRAINT,
// ADD FOREIGN KEY, DROP INDEX, DROP PRIMARY KEY.
var indexConstraintKeywords = map[string]struct{}{
	"INDEX": {}, "KEY": {}, "UNIQUE": {}, "FULLTEXT": {}, "SPATIAL": {},
	"PRIMARY": {}, "FOREIGN": {}, "CONSTRAINT": {}, "CHECK": {},
}

// isIndexOnlyTableAlteration reports whether an ALTER TABLE statement only adds,
// drops, renames, or toggles indexes/keys/constraints, never touching columns —
// none of which processAlterTableQuery acts on.
//
// It requires at least one index/key/constraint operation and refuses as soon as it
// sees any column operation, so a mixed statement like
//
//	ALTER TABLE t ADD c INT, ADD INDEX i (c)
//
// (which would silently lose the added column) is still reported.
func isIndexOnlyTableAlteration(kw []string) bool {
	sawIndexOp := false
	for i, w := range kw {
		next := ""
		if i+1 < len(kw) {
			next = kw[i+1]
		}
		switch w {
		case "MODIFY", "CHANGE":
			// always column operations (MODIFY/CHANGE [COLUMN] ...)
			return false
		case "COLUMN":
			// ALTER COLUMN / RENAME COLUMN — column ops whose verb isn't matched
			// above. (ADD/DROP COLUMN are handled by the ADD/DROP case; the COLUMN
			// keyword there is optional, so those bare forms land there too.)
			// e.g. "ALTER TABLE t ADD INDEX i (a), ALTER COLUMN b SET DEFAULT 1"
			return false
		case "ADD", "DROP":
			if _, ok := indexConstraintKeywords[next]; ok {
				sawIndexOp = true
			} else {
				// ADD/DROP of a column, with or without the optional COLUMN keyword
				return false
			}
		case "RENAME":
			// RENAME {INDEX|KEY} ... TO ... is an index op. RENAME COLUMN is caught by
			// the COLUMN case, and RENAME TO <table> (a table rename) is left to the
			// default and reported.
			if next == "INDEX" || next == "KEY" {
				sawIndexOp = true
			}
		case "ALTER":
			// ALTER INDEX ... [IN]VISIBLE toggles index visibility. The leading
			// "ALTER TABLE" has next == "TABLE", and ALTER COLUMN is caught by the
			// COLUMN case, so neither is misread as an index op here.
			if next == "INDEX" {
				sawIndexOp = true
			}
		case "DISABLE", "ENABLE":
			// {DISABLE|ENABLE} KEYS toggles index maintenance.
			if next == "KEYS" {
				sawIndexOp = true
			}
		}
	}
	return sawIndexOp
}

type ddlKind int

const (
	ddlKindIgnored ddlKind = iota
	ddlKindAlterTable
	ddlKindRenameTable
)

// classifyParsedStatement maps a successfully parsed statement to the handler that
// acts on it, returning the typed node for that handler (nil for the others). Every
// other statement — CREATE/DROP TABLE, DATABASE / SCHEMA / INDEX / VIEW /
// stored-routine / trigger / event / user DDL, SET, XA, GRANT/REVOKE, etc. — is
// ddlKindIgnored even when it parses cleanly.
func classifyParsedStatement(stmt ast.StmtNode) (ddlKind, *ast.AlterTableStmt, *ast.RenameTableStmt) {
	switch s := stmt.(type) {
	case *ast.AlterTableStmt:
		return ddlKindAlterTable, s, nil
	case *ast.RenameTableStmt:
		return ddlKindRenameTable, nil, s
	default:
		return ddlKindIgnored, nil, nil
	}
}

// classifyUnparsedStatement classifies a QueryEvent that failed to parse using only
// its leading keywords, the AST being unavailable. It mirrors classifyParsedStatement:
// ddlKindIgnored means PeerDB would not have acted on the statement even had it parsed,
// so the parse failure is benign noise; any other kind means a statement we care about
// failed to parse and the failure should be reported.
//
// The TiDB parser routinely rejects statements RDS/MariaDB emit constantly (e.g.
// SET STATEMENT ... FOR ... heartbeats, stored-routine bodies, MariaDB-only DDL),
// which are the bulk of the noise this drops.
func classifyUnparsedStatement(query string, isMariaDb bool) ddlKind {
	kw := leadingKeywords(query, 20)
	if len(kw) == 0 {
		return ddlKindIgnored
	}

	switch kw[0] {
	case "ALTER":
		if firstObjectKeyword(kw[1:]) != "TABLE" {
			return ddlKindIgnored
		}
		// An ALTER TABLE that only manipulates indexes/keys/constraints is a no-op for
		// processAlterTableQuery, so its parse failure is benign too.
		if isIndexOnlyTableAlteration(kw) {
			return ddlKindIgnored
		}
		return ddlKindAlterTable
	case "RENAME":
		// MariaDB also allows the plural "RENAME TABLES ...".
		objectKeyword := firstObjectKeyword(kw[1:])
		if objectKeyword == "TABLE" || (isMariaDb && objectKeyword == "TABLES") {
			return ddlKindRenameTable
		}
		return ddlKindIgnored
	default:
		// CREATE/DROP (of any object, including TABLE) and everything else are not processed.
		return ddlKindIgnored
	}
}
