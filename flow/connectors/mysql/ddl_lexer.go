package connmysql

import (
	"errors"
	"strings"
)

// sql_mode bits that change lexing, extracted per-event from binlog status vars.
// Bit positions agree between MySQL and MariaDB for all bits < 32; the layouts in
// mysql-server sql/system_variables.h and mariadb-server sql/sql_class.h are
// authoritative (the doc table in libbinlogevents statement_events.h is stale).
const (
	sqlModeANSIQuotes         uint64 = 1 << 2
	sqlModeOracle             uint64 = 1 << 9  // MariaDB only
	sqlModeMSSQL              uint64 = 1 << 10 // MariaDB only
	sqlModeNoBackslashEscapes uint64 = 1 << 20
)

type ddlTokenKind int8

const (
	tokEOF ddlTokenKind = iota
	tokErr
	tokWord        // unquoted identifier or keyword, text exactly as written
	tokQuotedIdent // `...`, "..." under ANSI_QUOTES, [...] under MSSQL; text is the decoded name
	tokString      // '...', "...", N'...', $tag$...$tag$; text is the raw content between delimiters
	tokPunct       // a single operator/punctuation byte
)

type ddlToken struct {
	text string
	pos  int
	kind ddlTokenKind
}

// ddlWordIs reports whether t is the unquoted keyword kw (ASCII case-insensitive;
// kw is uppercase). Quoted identifiers never match: quoting demotes a keyword to a
// plain identifier on both engines.
func ddlWordIs(t ddlToken, kw string) bool {
	return t.kind == tokWord && strings.EqualFold(t.text, kw)
}

var (
	errUnterminatedString      = errors.New("unterminated string literal")
	errUnterminatedQuotedIdent = errors.New("unterminated quoted identifier")
)

// ddlLexer replicates the server lexer's token boundaries for the byte-safe
// client charsets (utf8/utf8mb4/latin1/ascii): comments (including executable
// comments, whose bodies the source server ran and which are therefore lexed as
// code), strings under the per-event sql_mode, quoted identifiers, and identifier
// runs. Anything else is emitted as single punctuation bytes — the parser only
// needs balanced parens and commas from those.
type ddlLexer struct {
	s           string
	pos         int
	sqlMode     uint64
	execComment int // depth of executable comment bodies currently lexed as code
	isMariaDB   bool
}

func isDDLIdentByte(c byte) bool {
	return c == '_' || c == '$' || (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

// isDDLSpaceOrCntrl matches the server's my_isspace || my_iscntrl test used to
// decide whether "--" starts a line comment.
func isDDLSpaceOrCntrl(c byte) bool {
	return c == ' ' || c == 0x7f || c < 0x20
}

func (lx *ddlLexer) next() (ddlToken, error) {
	for {
		n := len(lx.s)
		if lx.pos >= n {
			return ddlToken{kind: tokEOF, pos: lx.pos}, nil
		}
		c := lx.s[lx.pos]
		switch {
		case c == 0:
			// an embedded NUL terminates the input, like the server's end-of-query state
			return ddlToken{kind: tokEOF, pos: lx.pos}, nil
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f':
			lx.pos++
		case c == '#':
			lx.skipLineComment()
		case c == '-' && lx.pos+1 < n && lx.s[lx.pos+1] == '-' &&
			(lx.pos+2 >= n || isDDLSpaceOrCntrl(lx.s[lx.pos+2])):
			lx.skipLineComment()
		case c == '/' && lx.pos+1 < n && lx.s[lx.pos+1] == '*':
			lx.scanComment()
		case c == '*' && lx.execComment > 0 && lx.pos+1 < n && lx.s[lx.pos+1] == '/':
			// closing an executable comment whose body is being lexed as code;
			// a */ inside a string never reaches here (strings are scanned whole)
			lx.execComment--
			lx.pos += 2
		case c == '\'':
			return lx.scanText('\'')
		case c == '"':
			if lx.sqlMode&sqlModeANSIQuotes != 0 {
				return lx.scanDelimitedIdent('"')
			}
			return lx.scanText('"')
		case c == '`':
			return lx.scanDelimitedIdent('`')
		case c == '[' && lx.isMariaDB && lx.sqlMode&sqlModeMSSQL != 0:
			return lx.scanDelimitedIdent(']')
		case c == '$' && !lx.isMariaDB:
			tok, ok, err := lx.scanDollarQuoted()
			if ok {
				return tok, err
			}
			return lx.scanWord(), nil
		case isDDLIdentByte(c):
			return lx.scanWord(), nil
		default:
			tok := ddlToken{kind: tokPunct, text: lx.s[lx.pos : lx.pos+1], pos: lx.pos}
			lx.pos++
			return tok, nil
		}
	}
}

// skipLineComment consumes a '#' or '--' comment. Line comments terminate at \n
// or NUL only — a bare \r does NOT terminate them in either server.
func (lx *ddlLexer) skipLineComment() {
	for lx.pos < len(lx.s) {
		c := lx.s[lx.pos]
		if c == 0 {
			return // NUL is consumed by the main loop as end of input
		}
		lx.pos++
		if c == '\n' {
			return
		}
	}
}

// scanComment handles "/*" openers. Executable comments (/*! on both engines,
// /*M! on MariaDB only) have their bodies lexed as code: source servers patch
// version comments they skipped into plain comments before binlogging, so any
// executable comment that survives into the binlog was executed. A 5-6 digit run
// after the marker is a version prefix and is skipped; fewer digits mean the body
// (digits included) is code. MariaDB /*!!NNNNN reverses the gate: surviving
// digits mean the source skipped it, while a digitless /*!! body was executed.
func (lx *ddlLexer) scanComment() {
	n := len(lx.s)
	body := lx.pos + 2
	switch {
	case body < n && lx.s[body] == '!':
		if lx.isMariaDB && body+1 < n && lx.s[body+1] == '!' {
			if d := countDDLDigits(lx.s, body+2); d == 5 || d == 6 {
				lx.skipPlainComment()
				return
			}
			lx.pos = body + 2
			lx.execComment++
			return
		}
		lx.pos = body + 1 + ddlVersionPrefixLen(countDDLDigits(lx.s, body+1))
		lx.execComment++
	case lx.isMariaDB && body+1 < n && lx.s[body] == 'M' && lx.s[body+1] == '!':
		lx.pos = body + 2 + ddlVersionPrefixLen(countDDLDigits(lx.s, body+2))
		lx.execComment++
	default:
		lx.skipPlainComment()
	}
}

func countDDLDigits(s string, pos int) int {
	d := 0
	for pos+d < len(s) && s[pos+d] >= '0' && s[pos+d] <= '9' {
		d++
	}
	return d
}

func ddlVersionPrefixLen(digits int) int {
	switch {
	case digits >= 6:
		return 6
	case digits == 5:
		return 5
	default:
		return 0
	}
}

// skipPlainComment consumes /* ... */ to the first */ — quote-unaware, no
// nesting, exactly like the server. Unterminated comments run to end of input.
func (lx *ddlLexer) skipPlainComment() {
	rest := lx.s[lx.pos+2:]
	end := strings.Index(rest, "*/")
	if nul := strings.IndexByte(rest, 0); nul >= 0 && (end < 0 || nul < end) {
		lx.pos += 2 + nul
		return
	}
	if end < 0 {
		lx.pos = len(lx.s)
		return
	}
	lx.pos += 2 + end + 2
}

// scanText scans a string literal. Doubling the delimiter always escapes it;
// backslash escapes the next byte unless NO_BACKSLASH_ESCAPES is set.
func (lx *ddlLexer) scanText(quote byte) (ddlToken, error) {
	start := lx.pos
	noBackslash := lx.sqlMode&sqlModeNoBackslashEscapes != 0
	n := len(lx.s)
	i := lx.pos + 1
	for i < n {
		c := lx.s[i]
		switch {
		case c == 0:
			return ddlToken{}, errUnterminatedString
		case c == quote:
			if i+1 < n && lx.s[i+1] == quote {
				i += 2
				continue
			}
			lx.pos = i + 1
			return ddlToken{kind: tokString, text: lx.s[start+1 : i], pos: start}, nil
		case c == '\\' && !noBackslash:
			i += 2
		default:
			i++
		}
	}
	return ddlToken{}, errUnterminatedString
}

// scanDelimitedIdent scans a quoted identifier (opener at lx.pos, closed by
// closer). Doubling escapes the closer; backslashes are never escapes here; NUL
// or end of input aborts, matching the server. The undoubled common case
// returns a zero-copy slice of lx.s; the Builder path runs only when a doubled
// closer is actually present.
func (lx *ddlLexer) scanDelimitedIdent(closer byte) (ddlToken, error) {
	start := lx.pos
	n := len(lx.s)
	i := lx.pos + 1
	for i < n {
		c := lx.s[i]
		if c == 0 {
			break
		}
		if c == closer {
			if i+1 < n && lx.s[i+1] == closer {
				return lx.scanDelimitedIdentDoubled(closer, i)
			}
			lx.pos = i + 1
			return ddlToken{kind: tokQuotedIdent, text: lx.s[start+1 : i], pos: start}, nil
		}
		i++
	}
	return ddlToken{}, errUnterminatedQuotedIdent
}

// scanDelimitedIdentDoubled finishes scanDelimitedIdent from the first doubled
// closer (at firstPair), decoding the doubling into a fresh string.
func (lx *ddlLexer) scanDelimitedIdentDoubled(closer byte, firstPair int) (ddlToken, error) {
	start := lx.pos
	var sb strings.Builder
	sb.WriteString(lx.s[start+1 : firstPair+1])
	n := len(lx.s)
	i := firstPair + 2
	for i < n {
		c := lx.s[i]
		if c == 0 {
			break
		}
		if c == closer {
			if i+1 < n && lx.s[i+1] == closer {
				sb.WriteByte(closer)
				i += 2
				continue
			}
			lx.pos = i + 1
			return ddlToken{kind: tokQuotedIdent, text: sb.String(), pos: start}, nil
		}
		sb.WriteByte(c)
		i++
	}
	return ddlToken{}, errUnterminatedQuotedIdent
}

// scanDollarQuoted recognizes MySQL 9 dollar-quoted strings $tag$...$tag$ at a
// token-start '$'. When the tag run is not closed by a second '$', the '$' begins
// an ordinary identifier and ok is false.
func (lx *ddlLexer) scanDollarQuoted() (ddlToken, bool, error) {
	n := len(lx.s)
	j := lx.pos + 1
	for j < n && isDDLIdentByte(lx.s[j]) && lx.s[j] != '$' {
		j++
	}
	if j >= n || lx.s[j] != '$' {
		return ddlToken{}, false, nil
	}
	tag := lx.s[lx.pos : j+1]
	rest := lx.s[j+1:]
	end := strings.Index(rest, tag)
	if end < 0 {
		return ddlToken{}, true, errUnterminatedString
	}
	if nul := strings.IndexByte(rest, 0); nul >= 0 && nul < end {
		return ddlToken{}, true, errUnterminatedString
	}
	tok := ddlToken{kind: tokString, text: rest[:end], pos: lx.pos}
	lx.pos = j + 1 + end + len(tag)
	return tok, true, nil
}

func (lx *ddlLexer) scanWord() ddlToken {
	start := lx.pos
	for lx.pos < len(lx.s) && isDDLIdentByte(lx.s[lx.pos]) {
		lx.pos++
	}
	return ddlToken{kind: tokWord, text: lx.s[start:lx.pos], pos: start}
}
