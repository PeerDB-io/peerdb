package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// lexTestCol parses a query expected to yield one ALTER TABLE whose first spec
// adds/modifies exactly one column, and returns that column.
func lexTestCol(t *testing.T, query string, isMariaDB bool) ddlColumnDef {
	t.Helper()
	alter := ddlParseAlter(t, query, 0, isMariaDB)
	require.Len(t, alter.Specs, 1)
	require.Len(t, alter.Specs[0].NewColumns, 1)
	return alter.Specs[0].NewColumns[0]
}

// lexTestBenign reports whether the query parses without error into nothing
// actionable (no rename, no alter spec with column content).
func lexTestBenign(t *testing.T, query string, isMariaDB bool) bool {
	t.Helper()
	stmts, err := parseQueryEvent([]byte(query), 0, isMariaDB)
	require.NoError(t, err)
	return !ddlIsActionable(stmts)
}

// TestDDLLexerExecCommentDigitRules pins the version-prefix digit rules through
// the type-parameter position: digits that are part of the version prefix are
// skipped, digits that are not become code and land in the type params.
// Fewer than 5 digits (including none) means "not a version comment" and the
// whole body, digits included, is code.
func TestDDLLexerExecCommentDigitRules(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		wantType  string
		isMariaDB bool
	}{
		{name: "bang zero digits body is code", query: "ALTER TABLE t ADD c VARCHAR(/*! 20 */)", wantType: "varchar(20)"},
		{name: "bang two digits are code", query: "ALTER TABLE t ADD c VARCHAR(/*!20*/)", wantType: "varchar(20)"},
		{name: "bang four digits are code", query: "ALTER TABLE t ADD c VARCHAR(/*!2000*/)", wantType: "varchar(2000)"},
		{name: "bang five digit version prefix skipped", query: "ALTER TABLE t ADD c VARCHAR(/*!80000 30*/)", wantType: "varchar(30)"},
		{
			name: "bang six digit version prefix skipped", isMariaDB: true,
			query: "ALTER TABLE t ADD c VARCHAR(/*!100000 40*/)", wantType: "varchar(40)",
		},
		{name: "mbang zero digits body is code", query: "ALTER TABLE t ADD c VARCHAR(/*M! 50*/)", wantType: "varchar(50)", isMariaDB: true},
		{name: "mbang four digits are code", query: "ALTER TABLE t ADD c VARCHAR(/*M!1234*/)", wantType: "varchar(1234)", isMariaDB: true},
		{
			name: "mbang five digit version prefix skipped", isMariaDB: true,
			query: "ALTER TABLE t ADD c VARCHAR(/*M!10050 60*/)", wantType: "varchar(60)",
		},
		// reversed comment with <5 digits is not a version comment either
		{name: "reversed four digits are code", query: "ALTER TABLE t ADD c VARCHAR(/*!!2000*/)", wantType: "varchar(2000)", isMariaDB: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantType, lexTestCol(t, tc.query, tc.isMariaDB).TypeStr)
		})
	}
}

func TestDDLLexerMySQLExecCommentSixthDigitBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		wantTable string
	}{
		{
			name:      "sixth digit without whitespace is body",
			query:     "ALTER TABLE  /*!100500r ADD COLUMN c INT */",
			wantTable: "0r",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alter := ddlParseAlter(t, tc.query, 0, false)
			require.Equal(t, tc.wantTable, alter.Table)
			require.Equal(t, ddlAltAdd(ddlAltCol("c", "int")), alter.Specs)
		})
	}
}

// TestDDLLexerExecCommentGating pins which /*-forms unwrap to code, observed
// through whether a NOT NULL inside the comment reaches the column.
func TestDDLLexerExecCommentGating(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		isMariaDB bool
		wantNN    bool
	}{
		// /*M! is MariaDB-only; MySQL sees an ordinary comment
		{name: "mbang is code on mariadb", query: "ALTER TABLE t ADD c INT /*M! NOT NULL */", isMariaDB: true, wantNN: true},
		{name: "mbang is plain comment on mysql", query: "ALTER TABLE t ADD c INT /*M! NOT NULL */", wantNN: false},
		// 5 digits not followed by a space: warning but the version is honored
		// the engine may warn, but the version prefix is still honored
		{name: "five digits without space still a version prefix", query: "ALTER TABLE t ADD c INT /*!80000NOT NULL*/", wantNN: true},
		// MariaDB has no whitespace-after-digits requirement at all
		{name: "mariadb six digits without space", query: "ALTER TABLE t ADD c INT /*M!100000NOT NULL*/", isMariaDB: true, wantNN: true},
		// surviving /*!! digits mean the source skipped the body (executed ones
		// get their digits patched to spaces before binlogging)
		{name: "reversed five digits is plain comment", query: "ALTER TABLE t ADD c INT /*!!11050 NOT NULL*/", isMariaDB: true, wantNN: false},
		{name: "reversed six digits is plain comment", query: "ALTER TABLE t ADD c INT /*!!110500 NOT NULL*/", isMariaDB: true, wantNN: false},
		{name: "reversed without digits is code", query: "ALTER TABLE t ADD c INT /*!! NOT NULL*/", isMariaDB: true, wantNN: true},
		// optimizer-hint syntax is an ordinary comment outside DML keyword position
		{name: "plus is plain comment", query: "ALTER TABLE t ADD c INT /*+ NOT NULL */", wantNN: false},
		{name: "empty comment", query: "ALTER TABLE t ADD c INT /**/ NOT NULL", wantNN: true},
		// the '/' right after the opener is content, not a closer
		{name: "slash after opener is content", query: "ALTER TABLE t ADD c INT /*/ NOT NULL */", wantNN: false},
		// unclosed executable comment: the server rejects it pre-binlog, so the
		// only requirement is the safety property (never silently benign)
		{name: "unterminated executable comment body still parses", query: "ALTER TABLE t ADD c INT /*!80000 NOT NULL", wantNN: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantNN, lexTestCol(t, tc.query, tc.isMariaDB).NotNull)
		})
	}

	// a string inside an executable comment body is lexed quote-aware: its */
	// must not close the comment
	col := lexTestCol(t, "/*!80000 ALTER TABLE t ADD c CHAR(9) DEFAULT 'x*/y' */", false)
	require.Equal(t, "char(9)", col.TypeStr)

	// mysqldump wraps index toggles in executable comments; the body lexes as
	// code and parses into a benign empty-spec alter
	alter := ddlParseAlter(t, "/*!40000 ALTER TABLE `t` DISABLE KEYS */", 0, false)
	require.Empty(t, alter.Specs)
}

// TestDDLLexerCommentBoundaries pins where plain comments end. Plain /*...*/
// is consumed to the FIRST */ — quote-unaware and with zero nesting
// (consume_comment(lip, 0); the one-level tolerance exists only for skipped
// version comments, which the source patches before binlogging).
func TestDDLLexerCommentBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		query  string
		benign bool
	}{
		{name: "plain comment is quote unaware", query: "/* it's */ ALTER TABLE t ADD COLUMN c INT", benign: false},
		{name: "comment ends at first close", query: "/* x /* y */ ALTER TABLE t ADD COLUMN c INT", benign: false},
		{name: "nested comment tail is head garbage", query: "/* a /* b */ c */ ALTER TABLE t ADD COLUMN c INT", benign: true},
		// line comments end at \n or NUL only; a bare \r stays inside
		{name: "hash comment cr does not terminate", query: "# note\rALTER TABLE t ADD COLUMN c INT", benign: true},
		{name: "hash comment lf terminates", query: "# note\nALTER TABLE t ADD COLUMN c INT", benign: false},
		// '--x' is two minus operators, so the statement head is not ALTER
		{name: "double dash without space at head is not a comment", query: "--x\nALTER TABLE t ADD COLUMN c INT", benign: true},
		{name: "unterminated comment after ignored head", query: "SELECT 1 /* unterminated", benign: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.benign, lexTestBenign(t, tc.query, false))
		})
	}

	// unterminated plain comment inside an actionable statement: rejected by the
	// server pre-binlog, so the only requirement is to never silently drop it
	alter := ddlParseAlter(t, "ALTER TABLE t DROP COLUMN c /* trailing", 0, false)
	require.Equal(t, []ddlAlterSpec{{OldColumnName: "c"}}, alter.Specs)
}

func TestDDLLexerLineComments(t *testing.T) {
	// '-- ' with a following space is a comment to end of line
	stmts, err := parseQueryEvent([]byte("RENAME TABLE a -- to c\nTO b"), 0, false)
	require.NoError(t, err)
	require.Equal(t, []ddlRenamePair{{OldTable: "a", NewTable: "b"}}, stmts[0].(*ddlRenameTable).Pairs)

	// a control char (tab) after '--' also starts a comment (my_isspace || my_iscntrl)
	stmts, err = parseQueryEvent([]byte("RENAME TABLE a --\tcomment\nTO b"), 0, false)
	require.NoError(t, err)
	require.Equal(t, []ddlRenamePair{{OldTable: "a", NewTable: "b"}}, stmts[0].(*ddlRenameTable).Pairs)

	// '--x' is two minus operators: garbage inside an actionable statement errors
	_, err = parseQueryEvent([]byte("RENAME TABLE a --x\nTO b"), 0, false)
	require.Error(t, err)

	// '--' at end of input starts an (empty) comment
	alter := ddlParseAlter(t, "ALTER TABLE t DROP COLUMN c --", 0, false)
	require.Equal(t, []ddlAlterSpec{{OldColumnName: "c"}}, alter.Specs)

	// '#' comment ends at \n; the spec list continues after it
	alter = ddlParseAlter(t, "ALTER TABLE t ADD c INT # tail\n, ADD d INT", 0, false)
	require.Len(t, alter.Specs, 2)
}

// TestDDLLexerStringModes exercises string literals under the four combinations
// of ANSI_QUOTES x NO_BACKSLASH_ESCAPES: doubling always escapes the delimiter;
// backslash escapes the next byte unless NO_BACKSLASH_ESCAPES (get_text).
func TestDDLLexerStringModes(t *testing.T) {
	const doubled = `ALTER TABLE t ADD c CHAR(9) DEFAULT 'it''s', ADD d INT`
	const bsQuote = `ALTER TABLE t ADD c CHAR(9) DEFAULT 'a\'b', ADD d INT`
	const dqString = `ALTER TABLE t ADD c CHAR(9) DEFAULT "a\"b", ADD d INT`
	const ansiNbe = `ALTER TABLE "t" ADD c CHAR(3) DEFAULT 'a\' NOT NULL`
	for _, tc := range []struct {
		name      string
		query     string
		sqlMode   uint64
		wantSpecs int
		wantErr   bool
	}{
		{name: "doubled quote always escapes", query: doubled, sqlMode: 0, wantSpecs: 2},
		{name: "doubled quote under no backslash escapes", query: doubled, sqlMode: sqlModeNoBackslashEscapes, wantSpecs: 2},
		{name: "backslash escapes the quote", query: bsQuote, sqlMode: 0, wantSpecs: 2},
		// under NO_BACKSLASH_ESCAPES the \ is literal, the string closes early and
		// the quote before the comma opens an unterminated string
		{name: "backslash literal splits the string", query: bsQuote, sqlMode: sqlModeNoBackslashEscapes, wantErr: true},
		{name: "double quoted string with backslash escape", query: dqString, sqlMode: 0, wantSpecs: 2},
		{name: "double quoted string no backslash escapes", query: dqString, sqlMode: sqlModeNoBackslashEscapes, wantErr: true},
		// both bits: "t" is an identifier and the trailing backslash is literal
		{name: "ansi quotes plus no backslash escapes", query: ansiNbe, sqlMode: sqlModeANSIQuotes | sqlModeNoBackslashEscapes, wantSpecs: 1},
		// ANSI_QUOTES alone keeps backslash escapes: 'a\' never terminates
		{name: "ansi quotes alone keeps backslash escapes", query: ansiNbe, sqlMode: sqlModeANSIQuotes, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parseQueryEvent([]byte(tc.query), tc.sqlMode, false)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.Len(t, stmts[0].(*ddlAlterTable).Specs, tc.wantSpecs)
		})
	}
}

func TestDDLLexerIdentifierForms(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		schema    string
		table     string
		col       string
		sqlMode   uint64
		isMariaDB bool
		wantErr   bool
	}{
		{
			name:  "backtick doubling and comment-looking content",
			query: "ALTER TABLE `x``y` DROP COLUMN `a--b/*c*/`", table: "x`y", col: "a--b/*c*/",
		},
		{
			name: "ansi quotes ident doubling", sqlMode: sqlModeANSIQuotes,
			query: `ALTER TABLE "my""tbl" DROP COLUMN "c"`, table: `my"tbl`, col: "c",
		},
		// no backslash escapes inside quoted identifiers, in any mode
		{
			name: "ansi ident backslash is literal", sqlMode: sqlModeANSIQuotes,
			query: `ALTER TABLE "a\" DROP COLUMN c`, table: `a\`, col: "c",
		},
		{name: "double quote is a string without ansi mode", query: `ALTER TABLE "t" DROP COLUMN c`, wantErr: true},
		{
			name: "mssql bracket ident with doubling", sqlMode: sqlModeMSSQL, isMariaDB: true,
			query: "ALTER TABLE [a]]b] DROP COLUMN [c]", table: "a]b", col: "c",
		},
		{name: "brackets need mssql mode", query: "ALTER TABLE [t] DROP COLUMN [c]", isMariaDB: true, wantErr: true},
		// digit-leading identifiers are legal (MY_LEX_NUMBER_IDENT)
		{name: "digit leading identifiers", query: "ALTER TABLE 23ai DROP COLUMN 1c", table: "23ai", col: "1c"},
		{name: "all digit identifier after dot", query: "ALTER TABLE db.123 DROP COLUMN c", schema: "db", table: "123", col: "c"},
		// '1e' without exponent digits is an identifier (the "1e1 test")
		{name: "1e alone is an identifier", query: "ALTER TABLE 1e DROP COLUMN c", table: "1e", col: "c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantErr {
				_, err := parseQueryEvent([]byte(tc.query), tc.sqlMode, tc.isMariaDB)
				require.Error(t, err)
				return
			}
			alter := ddlParseAlter(t, tc.query, tc.sqlMode, tc.isMariaDB)
			require.Equal(t, tc.schema, alter.Schema)
			require.Equal(t, tc.table, alter.Table)
			require.Equal(t, []ddlAlterSpec{{OldColumnName: tc.col}}, alter.Specs)
		})
	}
}

// TestDDLLexerLiteralForms feeds hex/bit/number literals, N-strings and charset
// introducers through DEFAULT: commas and keywords inside them must not leak
// out, so both specs and the trailing NOT NULL must survive.
func TestDDLLexerLiteralForms(t *testing.T) {
	for _, lit := range []string{
		"X'2C2C'", "x'2c'", "B'01'", "0b01", "0xAB",
		".5", "1.e5", "1e+5", "1e5",
		"N'a, TO b'", "_utf8mb4'a, TO b'", "_latin1 X'2C'",
	} {
		t.Run(lit, func(t *testing.T) {
			stmts, err := parseQueryEvent([]byte("ALTER TABLE t ADD c CHAR(9) DEFAULT "+lit+", ADD d INT NOT NULL"), 0, false)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			alter := stmts[0].(*ddlAlterTable)
			require.Len(t, alter.Specs, 2)
			require.Equal(t, []ddlColumnDef{{Name: "d", TypeStr: "int", Precision: -1, Scale: -1, NotNull: true}},
				alter.Specs[1].NewColumns)
		})
	}
}

// TestDDLLexerUserVariables: @'quoted', @`quoted` and @@system variables in a
// SET STATEMENT var list must be consumed opaquely — a FOR (or comma) inside
// them must not end the list or split values.
func TestDDLLexerUserVariables(t *testing.T) {
	stmts, err := parseQueryEvent([]byte("SET STATEMENT a=@'v, FOR x' FOR RENAME TABLE a TO b"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.IsType(t, &ddlRenameTable{}, stmts[0])

	// quoting demotes the keyword: @`FOR` is a variable name, not the FOR
	stmts, err = parseQueryEvent([]byte("SET STATEMENT a=@`FOR` FOR RENAME TABLE a TO b"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.IsType(t, &ddlRenameTable{}, stmts[0])

	stmts, err = parseQueryEvent(
		[]byte("SET STATEMENT a=@v, b=@@global.max_join_size FOR ALTER TABLE t ADD COLUMN c INT"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.IsType(t, &ddlAlterTable{}, stmts[0])
}

func TestDDLLexerDollarQuoting(t *testing.T) {
	// MySQL 9 dollar-quoted string: the comma inside must not split the specs
	alter := ddlParseAlter(t, "ALTER TABLE t ADD c VARCHAR(20) DEFAULT $q$a, b$q$, ADD d INT", 0, false)
	require.Len(t, alter.Specs, 2)

	// '$' not opening a tag begins an ordinary identifier
	col := lexTestCol(t, "ALTER TABLE t ADD $c INT", false)
	require.Equal(t, "$c", col.Name)

	// unterminated dollar quote inside an actionable statement
	_, err := parseQueryEvent([]byte("ALTER TABLE t ADD c INT DEFAULT $q$oops"), 0, false)
	require.Error(t, err)

	// MariaDB has no dollar quoting: the apostrophe opens a string that never closes
	_, err = parseQueryEvent([]byte("ALTER TABLE t ADD c VARCHAR(20) DEFAULT $q$it's$q$"), 0, true)
	require.Error(t, err)
}

func TestDDLLexerNulHandling(t *testing.T) {
	// an embedded NUL terminates the input mid-statement (state_map[0] is end-of-query)
	alter := ddlParseAlter(t, "ALTER TABLE t DROP COLUMN a\x00, ADD COLUMN b INT", 0, false)
	require.Equal(t, []ddlAlterSpec{{OldColumnName: "a"}}, alter.Specs)

	// NUL aborts a quoted identifier inside an actionable statement
	_, err := parseQueryEvent([]byte("ALTER TABLE `a\x00b` DROP COLUMN c"), 0, false)
	require.Error(t, err)

	// but inside a string literal NUL is an ordinary byte: both servers scan
	// string bodies with a pointer-based end-of-input check
	stmtsNul, err := parseQueryEvent([]byte("ALTER TABLE t ADD c INT COMMENT 'a\x00b'"), 0, false)
	require.NoError(t, err)
	require.Len(t, stmtsNul, 1)

	// NUL inside a line comment ends the comment and the whole input
	stmts, err := parseQueryEvent([]byte("ALTER TABLE t DROP COLUMN a; -- x\x00ALTER TABLE t DROP COLUMN b"), 0, false)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
}
