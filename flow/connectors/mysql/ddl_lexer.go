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
	s             string
	pos           int
	sqlMode       uint64
	execComment   int  // depth of executable comment bodies currently lexed as code
	identAfterDot bool // server lexes the immediate ident after '.' in qualified names as an identifier
	isMariaDB     bool
}

func isDDLIdentByte(c byte) bool {
	return c == '_' || c == '$' || (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isDDLDigitByte(c byte) bool {
	return c >= '0' && c <= '9'
}

func isDDLHexDigitByte(c byte) bool {
	return isDDLDigitByte(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
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
		case lx.identAfterDot && isDDLIdentByte(c):
			lx.identAfterDot = false
			return lx.scanWord(), nil
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
		case isDDLDigitByte(c):
			return lx.scanNumberOrWord(), nil
		case isDDLIdentByte(c):
			return lx.scanWord(), nil
		default:
			tok := ddlToken{kind: tokPunct, text: lx.s[lx.pos : lx.pos+1], pos: lx.pos}
			lx.identAfterDot = c == '.' && lx.pos+1 < n && isDDLIdentByte(lx.s[lx.pos+1])
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
// /*M! on MariaDB only) have their bodies lexed as code when the server would
// execute them. A version prefix is skipped before lexing the body: MariaDB
// accepts 5 or 6 digits, while MySQL accepts a sixth digit only when it is
// followed by whitespace. Fewer digits mean the body (digits included) is code.
// MariaDB skips MySQL-style 5.7..9.x gates (50700..99999) but not /*M! gates.
// MariaDB /*!!NNNNN and /*M!!NNNNN reverse the gate: surviving digits mean the
// source skipped it, while a digitless body was executed.
func (lx *ddlLexer) scanComment() {
	n := len(lx.s)
	body := lx.pos + 2
	switch {
	case body < n && lx.s[body] == '!':
		if lx.isMariaDB && body+1 < n && lx.s[body+1] == '!' {
			if l, _ := ddlVersionPrefix(lx.s, body+2, true); l > 0 {
				lx.skipPlainComment()
				return
			}
			lx.pos = body + 2
			lx.execComment++
			return
		}
		versionLen, version := ddlVersionPrefix(lx.s, body+1, lx.isMariaDB)
		if lx.isMariaDB && version >= 50700 && version <= 99999 {
			lx.skipPlainComment()
			return
		}
		lx.pos = body + 1 + versionLen
		lx.execComment++
	case lx.isMariaDB && body+1 < n && lx.s[body] == 'M' && lx.s[body+1] == '!':
		versionPos := body + 2
		if versionPos < n && lx.s[versionPos] == '!' {
			if l, _ := ddlVersionPrefix(lx.s, versionPos+1, true); l > 0 {
				lx.skipPlainComment()
				return
			}
			versionPos++
		}
		versionLen, _ := ddlVersionPrefix(lx.s, versionPos, true)
		lx.pos = versionPos + versionLen
		lx.execComment++
	default:
		lx.skipPlainComment()
	}
}

func isDDLVersionSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

func isDDLDigitAt(s string, pos int) bool {
	return pos < len(s) && s[pos] >= '0' && s[pos] <= '9'
}

func ddlVersionPrefix(s string, pos int, isMariaDB bool) (int, int) {
	for i := range 5 {
		if !isDDLDigitAt(s, pos+i) {
			return 0, 0
		}
	}
	version := 0
	for i := range 5 {
		version = version*10 + int(s[pos+i]-'0')
	}
	if isMariaDB && isDDLDigitAt(s, pos+5) {
		return 6, version*10 + int(s[pos+5]-'0')
	}
	if !isMariaDB && isDDLDigitAt(s, pos+5) && pos+6 < len(s) && isDDLVersionSpace(s[pos+6]) {
		return 6, version*10 + int(s[pos+5]-'0')
	}
	return 5, version
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
// backslash escapes the next byte unless NO_BACKSLASH_ESCAPES is set. A NUL
// is an ordinary byte here: both servers scan string bodies with a
// pointer-based end-of-input check, unlike token-start position where NUL
// ends the query.
func (lx *ddlLexer) scanText(quote byte) (ddlToken, error) {
	start := lx.pos
	noBackslash := lx.sqlMode&sqlModeNoBackslashEscapes != 0
	n := len(lx.s)
	i := lx.pos + 1
	for i < n {
		c := lx.s[i]
		switch {
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

func scanDDLExponent(s string, pos int) (int, bool) {
	if pos >= len(s) || (s[pos] != 'e' && s[pos] != 'E') {
		return pos, false
	}
	i := pos + 1
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	if i >= len(s) || !isDDLDigitByte(s[i]) {
		return pos, false
	}
	for i < len(s) && isDDLDigitByte(s[i]) {
		i++
	}
	return i, true
}

func (lx *ddlLexer) scanNumberOrWord() ddlToken {
	start := lx.pos
	n := len(lx.s)
	if start+1 < n && lx.s[start] == '0' {
		switch lx.s[start+1] {
		case 'x', 'X':
			i := start + 2
			for i < n && isDDLHexDigitByte(lx.s[i]) {
				i++
			}
			if i > start+2 && (i >= n || !isDDLIdentByte(lx.s[i])) {
				lx.pos = i
				return ddlToken{kind: tokWord, text: lx.s[start:i], pos: start}
			}
			return lx.scanWord()
		case 'b', 'B':
			i := start + 2
			for i < n && (lx.s[i] == '0' || lx.s[i] == '1') {
				i++
			}
			if i > start+2 && (i >= n || !isDDLIdentByte(lx.s[i])) {
				lx.pos = i
				return ddlToken{kind: tokWord, text: lx.s[start:i], pos: start}
			}
			return lx.scanWord()
		}
	}

	i := start
	for i < n && isDDLDigitByte(lx.s[i]) {
		i++
	}
	if i < n && lx.s[i] == '.' && !(i+1 < n && lx.s[i+1] == '.') {
		i++
		for i < n && isDDLDigitByte(lx.s[i]) {
			i++
		}
		if end, ok := scanDDLExponent(lx.s, i); ok {
			i = end
		}
		lx.pos = i
		return ddlToken{kind: tokWord, text: lx.s[start:i], pos: start}
	}
	if end, ok := scanDDLExponent(lx.s, i); ok {
		lx.pos = end
		return ddlToken{kind: tokWord, text: lx.s[start:end], pos: start}
	}
	if i < n && isDDLIdentByte(lx.s[i]) {
		return lx.scanWord()
	}
	lx.pos = i
	return ddlToken{kind: tokWord, text: lx.s[start:i], pos: start}
}

func (lx *ddlLexer) scanWord() ddlToken {
	start := lx.pos
	for lx.pos < len(lx.s) && isDDLIdentByte(lx.s[lx.pos]) {
		lx.pos++
	}
	return ddlToken{kind: tokWord, text: lx.s[start:lx.pos], pos: start}
}
