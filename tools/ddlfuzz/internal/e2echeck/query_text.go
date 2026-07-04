package e2echeck

import "strings"

const sqlBoundaryWhitespace = " \t\n\r\f\v"

func EquivalentQueryText(a, b string, isMariaDB bool) bool {
	a = normalizeQueryTextBoundary(a)
	b = normalizeQueryTextBoundary(b)
	if equivalentNormalizedQueryText(a, b, isMariaDB) {
		return true
	}
	return queryTextWithOnlyEmptyTail(a, b, isMariaDB) || queryTextWithOnlyEmptyTail(b, a, isMariaDB)
}

func equivalentNormalizedQueryText(a, b string, isMariaDB bool) bool {
	return a == b || isMariaDB && plainifyMariaDBSkippedComments(a) == plainifyMariaDBSkippedComments(b)
}

func normalizeQueryTextBoundary(s string) string {
	s = strings.Trim(s, sqlBoundaryWhitespace)
	if strings.HasSuffix(s, ";") {
		s = strings.Trim(s[:len(s)-1], sqlBoundaryWhitespace)
	}
	return s
}

func queryTextWithOnlyEmptyTail(candidate, base string, isMariaDB bool) bool {
	for i := 0; i < len(candidate); {
		switch candidate[i] {
		case '\'', '"', '`':
			i = skipQuotedQueryText(candidate, i, candidate[i])
			continue
		case '[':
			i = skipQuotedQueryText(candidate, i, ']')
			continue
		case '#':
			i = skipLineCommentQueryText(candidate, i+1)
			continue
		case '-':
			if i+2 < len(candidate) && candidate[i+1] == '-' && isSQLBoundaryWhitespace(candidate[i+2]) {
				i = skipLineCommentQueryText(candidate, i+3)
				continue
			}
		case '/':
			if i+1 < len(candidate) && candidate[i+1] == '*' {
				i = skipBlockCommentQueryText(candidate, i+2)
				continue
			}
		case ';':
			head := normalizeQueryTextBoundary(candidate[:i+1])
			if equivalentNormalizedQueryText(head, base, isMariaDB) && onlyEmptyQueryTextStatements(candidate[i+1:]) {
				return true
			}
		}
		i++
	}
	return false
}

func onlyEmptyQueryTextStatements(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != ';' && !isSQLBoundaryWhitespace(s[i]) {
			return false
		}
	}
	return true
}

func skipLineCommentQueryText(s string, pos int) int {
	for pos < len(s) && s[pos] != '\n' && s[pos] != 0 {
		pos++
	}
	return pos
}

func skipBlockCommentQueryText(s string, pos int) int {
	if end := strings.Index(s[pos:], "*/"); end >= 0 {
		return pos + end + 2
	}
	return len(s)
}

func isSQLBoundaryWhitespace(c byte) bool {
	return strings.IndexByte(sqlBoundaryWhitespace, c) >= 0
}

func plainifyMariaDBSkippedComments(s string) string {
	var out []byte
	for i := 0; i+3 < len(s); {
		switch s[i] {
		case '\'', '"', '`':
			i = skipQuotedQueryText(s, i, s[i])
			continue
		case '[':
			i = skipQuotedQueryText(s, i, ']')
			continue
		}
		if s[i] != '/' || s[i+1] != '*' {
			i++
			continue
		}
		end := strings.Index(s[i+2:], "*/")
		out = plainifyMariaDBSkippedCommentMarker(s, out, i)
		if end < 0 {
			break
		}
		i += 2 + end + 2
	}
	if out == nil {
		return s
	}
	return string(out)
}

func plainifyMariaDBSkippedCommentMarker(s string, out []byte, i int) []byte {
	if i+3 >= len(s) || s[i] != '/' || s[i+1] != '*' || s[i+2] != '!' {
		return out
	}
	if s[i+3] == '!' && mariaSkippedVersionLen(s, i+4) > 0 {
		out = ensureQueryTextCopy(s, out)
		out[i+2] = ' '
		out[i+3] = ' '
		return out
	}
	if mariaSkippedVersionLen(s, i+3) == 0 {
		return out
	}
	out = ensureQueryTextCopy(s, out)
	out[i+2] = ' '
	return out
}

func ensureQueryTextCopy(s string, out []byte) []byte {
	if out != nil {
		return out
	}
	cp := []byte(s)
	return cp
}

func skipQuotedQueryText(s string, pos int, closer byte) int {
	i := pos + 1
	for i < len(s) {
		switch s[i] {
		case closer:
			if i+1 < len(s) && s[i+1] == closer {
				i += 2
				continue
			}
			return i + 1
		case '\\':
			i += 2
		default:
			i++
		}
	}
	return len(s)
}

func mariaSkippedVersionLen(s string, pos int) int {
	for i := range 5 {
		if pos+i >= len(s) || s[pos+i] < '0' || s[pos+i] > '9' {
			return 0
		}
	}
	if pos+5 < len(s) && s[pos+5] >= '0' && s[pos+5] <= '9' {
		return 6
	}
	return 5
}
