package e2echeck

import "strings"

// ExpectedEventSQLModeRelevant returns the relevant sql_mode bits a QueryEvent
// should carry for a submitted statement.
func ExpectedEventSQLModeRelevant(submitted string, sessionRelevant uint64, isMariaDB bool) uint64 {
	sessionRelevant &= RelevantSQLModeMask
	if !isMariaDB {
		return sessionRelevant
	}
	prefix, ok := setStatementPrefix(submitted, sessionRelevant)
	if !ok {
		return sessionRelevant
	}
	mode, ok := setStatementSQLMode(prefix, sessionRelevant, isMariaDB)
	if !ok {
		return sessionRelevant
	}
	return mode & RelevantSQLModeMask
}

func setStatementPrefix(sql string, mode uint64) (string, bool) {
	pos := skipSQLSpace(sql, 0)
	next, ok := consumeSQLWord(sql, pos, "SET")
	if !ok {
		return "", false
	}
	pos = skipSQLSpace(sql, next)
	next, ok = consumeSQLWord(sql, pos, "STATEMENT")
	if !ok {
		return "", false
	}

	start := next
	depth := 0
	for pos = next; pos < len(sql); {
		if next, ok := skipSQLComment(sql, pos); ok {
			pos = next
			continue
		}
		c := sql[pos]
		switch {
		case c == '\'' || c == '"' || c == '`':
			next, ok := skipSQLQuoted(sql, pos, c, mode)
			if !ok {
				return "", false
			}
			pos = next
		case c == '(':
			depth++
			pos++
		case c == ')':
			if depth > 0 {
				depth--
			}
			pos++
		case depth == 0 && isSQLWordStart(c):
			next := scanSQLWord(sql, pos)
			if strings.EqualFold(sql[pos:next], "FOR") {
				return sql[start:pos], true
			}
			pos = next
		default:
			pos++
		}
	}
	return "", false
}

func setStatementSQLMode(prefix string, sessionRelevant uint64, isMariaDB bool) (uint64, bool) {
	mode := sessionRelevant & RelevantSQLModeMask
	changed := false
	for _, part := range splitTopLevelComma(prefix, sessionRelevant) {
		name, expr, ok := splitSetAssignment(part, sessionRelevant)
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "sql_mode") {
			continue
		}
		next, ok := evalSQLModeExpr(expr, mode, sessionRelevant, isMariaDB)
		if !ok {
			return 0, false
		}
		mode = next & RelevantSQLModeMask
		changed = true
	}
	return mode, changed
}

func splitSetAssignment(part string, mode uint64) (string, string, bool) {
	depth := 0
	for pos := 0; pos < len(part); {
		if next, ok := skipSQLComment(part, pos); ok {
			pos = next
			continue
		}
		c := part[pos]
		switch {
		case c == '\'' || c == '"' || c == '`':
			next, ok := skipSQLQuoted(part, pos, c, mode)
			if !ok {
				return "", "", false
			}
			pos = next
		case c == '(':
			depth++
			pos++
		case c == ')':
			if depth > 0 {
				depth--
			}
			pos++
		case c == '=' && depth == 0:
			return part[:pos], part[pos+1:], true
		default:
			pos++
		}
	}
	return "", "", false
}

func evalSQLModeExpr(expr string, current, sessionRelevant uint64, isMariaDB bool) (uint64, bool) {
	expr = strings.TrimSpace(expr)
	if s, ok := decodeSQLStringLiteral(expr, sessionRelevant); ok {
		return sqlModeNamesRelevant(s), true
	}
	if !hasSQLFuncPrefix(expr, "CONCAT") {
		return 0, false
	}
	open := strings.IndexByte(expr, '(')
	close := strings.LastIndex(expr, ")")
	if open < 0 || close < open || strings.TrimSpace(expr[close+1:]) != "" {
		return 0, false
	}
	mode := uint64(0)
	used := false
	for _, arg := range splitTopLevelComma(expr[open+1:close], sessionRelevant) {
		arg = strings.TrimSpace(arg)
		if body, ok := unwrapExecutableSQLComment(arg, isMariaDB); ok {
			arg = strings.TrimSpace(body)
		}
		switch {
		case strings.EqualFold(arg, "@@sql_mode"), strings.EqualFold(arg, "@@session.sql_mode"):
			mode |= current & RelevantSQLModeMask
			used = true
		default:
			s, ok := decodeSQLStringLiteral(arg, sessionRelevant)
			if !ok {
				return 0, false
			}
			mode |= sqlModeNamesRelevant(s)
			used = true
		}
	}
	return mode & RelevantSQLModeMask, used
}

func unwrapExecutableSQLComment(s string, isMariaDB bool) (string, bool) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "/*") {
		return "", false
	}
	action, body := classifyExecutableCommentForQueryText(s, 0, isMariaDB)
	if action != queryTextCommentExecutable {
		return "", false
	}
	closeRel := strings.Index(s[body:], "*/")
	if closeRel < 0 {
		return "", false
	}
	close := body + closeRel
	if strings.TrimSpace(s[close+2:]) != "" {
		return "", false
	}
	return s[body:close], true
}

func hasSQLFuncPrefix(expr, name string) bool {
	if len(expr) < len(name)+1 || !strings.EqualFold(expr[:len(name)], name) {
		return false
	}
	rest := strings.TrimSpace(expr[len(name):])
	return strings.HasPrefix(rest, "(")
}

func sqlModeNamesRelevant(s string) uint64 {
	var out uint64
	for _, raw := range strings.Split(s, ",") {
		switch strings.ToUpper(strings.TrimSpace(raw)) {
		case "REAL_AS_FLOAT":
			out |= SQLModeRealAsFloat
		case "ANSI_QUOTES":
			out |= SQLModeANSIQuotes
		case "ORACLE":
			out |= SQLModeOracle
		case "MSSQL":
			out |= SQLModeMSSQL
		case "NO_BACKSLASH_ESCAPES":
			out |= SQLModeNoBackslashEscapes
		}
	}
	return out & RelevantSQLModeMask
}

// SQLModeNames renders the relevant bits of mode as a canonical
// comma-separated sql_mode value ("" for zero).
func SQLModeNames(mode uint64) string {
	mode &= RelevantSQLModeMask
	var names []string
	if mode&SQLModeRealAsFloat != 0 {
		names = append(names, "REAL_AS_FLOAT")
	}
	if mode&SQLModeANSIQuotes != 0 {
		names = append(names, "ANSI_QUOTES")
	}
	if mode&SQLModeOracle != 0 {
		names = append(names, "ORACLE")
	}
	if mode&SQLModeMSSQL != 0 {
		names = append(names, "MSSQL")
	}
	if mode&SQLModeNoBackslashEscapes != 0 {
		names = append(names, "NO_BACKSLASH_ESCAPES")
	}
	return strings.Join(names, ",")
}

func splitTopLevelComma(s string, mode uint64) []string {
	var parts []string
	start := 0
	depth := 0
	for pos := 0; pos < len(s); {
		if next, ok := skipSQLComment(s, pos); ok {
			pos = next
			continue
		}
		c := s[pos]
		switch {
		case c == '\'' || c == '"' || c == '`':
			next, ok := skipSQLQuoted(s, pos, c, mode)
			if !ok {
				return []string{s}
			}
			pos = next
		case c == '(':
			depth++
			pos++
		case c == ')':
			if depth > 0 {
				depth--
			}
			pos++
		case c == ',' && depth == 0:
			parts = append(parts, s[start:pos])
			pos++
			start = pos
		default:
			pos++
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func decodeSQLStringLiteral(s string, mode uint64) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == 'N' || s[0] == 'n') && s[1] == '\'' {
		s = s[1:]
	}
	if len(s) >= 2 && s[0] == '_' && isSQLWordStart(s[1]) {
		end := scanSQLWord(s, 1)
		if end < len(s) && (s[end] == '\'' || s[end] == '"') {
			s = s[end:]
		}
	}
	if len(s) < 2 {
		return "", false
	}
	quote := s[0]
	if quote != '\'' && quote != '"' {
		return "", false
	}
	if quote == '"' && mode&SQLModeANSIQuotes != 0 {
		return "", false
	}
	if s[len(s)-1] != quote {
		return "", false
	}
	var b strings.Builder
	for i := 1; i < len(s)-1; {
		c := s[i]
		if c == quote {
			if i+1 < len(s)-1 && s[i+1] == quote {
				b.WriteByte(quote)
				i += 2
				continue
			}
			return "", false
		}
		if c == '\\' && mode&SQLModeNoBackslashEscapes == 0 && i+1 < len(s)-1 {
			b.WriteByte(s[i+1])
			i += 2
			continue
		}
		b.WriteByte(c)
		i++
	}
	return b.String(), true
}

func skipSQLQuoted(s string, pos int, quote byte, mode uint64) (int, bool) {
	for i := pos + 1; i < len(s); {
		c := s[i]
		if c == quote {
			if i+1 < len(s) && s[i+1] == quote {
				i += 2
				continue
			}
			return i + 1, true
		}
		if c == '\\' && quote != '`' && !(quote == '"' && mode&SQLModeANSIQuotes != 0) && mode&SQLModeNoBackslashEscapes == 0 && i+1 < len(s) {
			i += 2
			continue
		}
		i++
	}
	return len(s), false
}

func skipSQLComment(s string, pos int) (int, bool) {
	if pos >= len(s) {
		return pos, false
	}
	switch s[pos] {
	case '#':
		return skipLineComment(s, pos+1), true
	case '-':
		if pos+2 < len(s) && s[pos+1] == '-' && isSQLSpaceOrControl(s[pos+2]) {
			return skipLineComment(s, pos+2), true
		}
	case '/':
		if pos+1 < len(s) && s[pos+1] == '*' {
			end := strings.Index(s[pos+2:], "*/")
			if end < 0 {
				return len(s), true
			}
			return pos + 2 + end + 2, true
		}
	}
	return pos, false
}

func skipLineComment(s string, pos int) int {
	for pos < len(s) && s[pos] != '\n' && s[pos] != 0 {
		pos++
	}
	return pos
}

func skipSQLSpace(s string, pos int) int {
	for pos < len(s) {
		switch s[pos] {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			pos++
		default:
			return pos
		}
	}
	return pos
}

func consumeSQLWord(s string, pos int, word string) (int, bool) {
	if pos+len(word) > len(s) || !strings.EqualFold(s[pos:pos+len(word)], word) {
		return pos, false
	}
	next := pos + len(word)
	if next < len(s) && isSQLWordPart(s[next]) {
		return pos, false
	}
	return next, true
}

func scanSQLWord(s string, pos int) int {
	for pos < len(s) && isSQLWordPart(s[pos]) {
		pos++
	}
	return pos
}

func isSQLWordStart(c byte) bool {
	return c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
}

func isSQLWordPart(c byte) bool {
	return isSQLWordStart(c) || c >= '0' && c <= '9' || c == '$'
}

func isSQLSpaceOrControl(c byte) bool {
	return c <= ' '
}
