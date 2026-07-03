package e2echeck

import "strings"

const sqlBoundaryWhitespace = " \t\n\r\f\v"

func EquivalentQueryText(a, b string, isMariaDB bool) bool {
	a = strings.Trim(a, sqlBoundaryWhitespace)
	b = strings.Trim(b, sqlBoundaryWhitespace)
	if a == b {
		return true
	}
	if !isMariaDB {
		return false
	}
	return plainifyMariaDBReversedSkippedComments(a) == b ||
		a == plainifyMariaDBReversedSkippedComments(b)
}

func plainifyMariaDBReversedSkippedComments(s string) string {
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
		if end < 0 {
			return stringWithReversedCommentPlainified(s, out, i)
		}
		if s[i+2] == '!' && s[i+3] == '!' && mariaReversedVersionLen(s, i+4) > 0 {
			out = ensureQueryTextCopy(s, out)
			out[i+2] = ' '
			out[i+3] = ' '
		}
		i += 2 + end + 2
	}
	if out == nil {
		return s
	}
	return string(out)
}

func stringWithReversedCommentPlainified(s string, out []byte, i int) string {
	if i+3 >= len(s) || s[i] != '/' || s[i+1] != '*' || s[i+2] != '!' || s[i+3] != '!' ||
		mariaReversedVersionLen(s, i+4) == 0 {
		if out == nil {
			return s
		}
		return string(out)
	}
	out = ensureQueryTextCopy(s, out)
	out[i+2] = ' '
	out[i+3] = ' '
	return string(out)
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

func mariaReversedVersionLen(s string, pos int) int {
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
