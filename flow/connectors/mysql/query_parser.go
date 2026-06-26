package connmysql

import "strings"

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
	"TABLE": {}, "DATABASE": {}, "SCHEMA": {}, "INDEX": {}, "VIEW": {},
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

// isBenignUnparsedStatement reports whether a QueryEvent that failed to parse is a
// statement kind PeerDB never acts on, so its parse failure is expected noise that
// should not be counted in ParseSQLErrorsCounter.
//
// The QueryEvent handler only processes ALTER TABLE and RENAME TABLE (see the switch
// in PullRecords). Every other statement — CREATE/DROP TABLE, any DATABASE / SCHEMA /
// INDEX / VIEW / stored-routine / trigger / event / user DDL, SET, XA, GRANT/REVOKE,
// etc. — is ignored even when it parses cleanly, so a parse failure on it cannot cause
// schema divergence. The TiDB parser routinely rejects such statements, which RDS/
// MariaDB emit constantly (e.g. SET STATEMENT ... FOR ... heartbeats), so they are
// the bulk of the noise we want to drop.
//
// Only ALTER TABLE / RENAME TABLE failures are reported. If the handler ever starts
// acting on more statement kinds, this allowlist must grow to match.
func isBenignUnparsedStatement(query string) bool {
	kw := leadingKeywords(query, 20)
	if len(kw) == 0 {
		return true
	}

	switch kw[0] {
	case "ALTER", "RENAME":
		// ALTER TABLE and RENAME TABLE are processed; ALTER/RENAME of any other object
		// (DATABASE, SCHEMA, INDEX, VIEW, EVENT, USER, ...) is not.
		return firstObjectKeyword(kw[1:]) != "TABLE"
	default:
		// CREATE/DROP (of any object, including TABLE) and everything else are not processed.
		return true
	}
}
