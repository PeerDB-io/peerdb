package connmysql

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ddlStatement interface{ isDDLStatement() }

type ddlColumnDef struct {
	Name      string
	TypeStr   string // normalized: lowercase canonical base [+ "(params)" if written] [+ " unsigned"]
	Precision int    // first numeric type param as written; -1 if not written
	Scale     int    // second numeric type param as written; -1 if not written
	NotNull   bool
}

type ddlAlterSpec struct {
	OldColumnName  string         // CHANGE old name, DROP [COLUMN] name, RENAME COLUMN old name
	NewColumnName  string         // RENAME COLUMN new name
	NewColumns     []ddlColumnDef // ADD (single or parenthesized multi), MODIFY, CHANGE
	AddIfNotExists bool
	ModifyIfExists bool
	ChangeColumn   bool // true for CHANGE, including changes from an empty identifier
	RenameColumn   bool // true for RENAME COLUMN, including renames to an empty identifier
	HasPosition    bool // a FIRST/AFTER placement was written on this spec
}

type ddlAlterTable struct {
	Schema string // empty when the table ident is unqualified
	Table  string
	Specs  []ddlAlterSpec // only column-relevant specs; index/constraint/option/partition specs are consumed and dropped
}

type ddlRenamePair struct{ OldSchema, OldTable, NewSchema, NewTable string }

type ddlRenameTable struct{ Pairs []ddlRenamePair }

func (*ddlAlterTable) isDDLStatement()  {}
func (*ddlRenameTable) isDDLStatement() {}

// ddlParser is a recursive-descent parser over ddlLexer tokens with unbounded
// (in practice LL(3)) lookahead. The first lexer error is latched: peeking past
// it yields tokErr, which actionable-statement parse paths surface as an error
// while head classification treats it as end of input.
type ddlParser struct {
	lexErr error
	buf    []ddlToken
	lx     ddlLexer
}

// parseQueryEvent parses a binlog QueryEvent's statement text into the DDL
// statements PeerDB acts on. Ignored statements yield nothing. err != nil ONLY
// when a statement whose head is ALTER ... TABLE or RENAME TABLE|TABLES fails to
// parse — safety property: an actionable-headed statement must never be silently
// ignored; benign statements never error.
func parseQueryEvent(query []byte, sqlMode uint64, isMariaDB bool) ([]ddlStatement, error) {
	text := string(bytes.TrimRight(query, "\x00"))
	p := &ddlParser{lx: ddlLexer{s: text, sqlMode: sqlMode, isMariaDB: isMariaDB}}
	var stmts []ddlStatement
	for {
		t := p.peek(0)
		if t.kind == tokPunct && t.text == ";" {
			p.next()
			continue
		}
		if t.kind != tokWord {
			// end of input, a lexer error before any actionable head, or a
			// statement starting with something other than a keyword
			return stmts, nil
		}
		switch {
		case ddlWordIs(t, "ALTER"):
			i := 1
			for ddlWordIs(p.peek(i), "ONLINE") || ddlWordIs(p.peek(i), "IGNORE") {
				i++
			}
			if !ddlWordIs(p.peek(i), "TABLE") {
				return stmts, nil
			}
			parsed, err := p.parseAlterTable()
			if err != nil {
				return nil, fmt.Errorf("ALTER TABLE parse failed: %w", err)
			}
			stmts = append(stmts, parsed...)
		case ddlWordIs(t, "RENAME"):
			// RENAME TABLES is valid (undocumented) MySQL too, not just MariaDB
			if !ddlWordIs(p.peek(1), "TABLE") && !ddlWordIs(p.peek(1), "TABLES") {
				return stmts, nil // RENAME USER and anything else
			}
			stmt, err := p.parseRenameTable()
			if err != nil {
				return nil, fmt.Errorf("RENAME TABLE parse failed: %w", err)
			}
			if len(stmt.Pairs) > 0 {
				stmts = append(stmts, stmt)
			}
		case ddlWordIs(t, "SET") && ddlWordIs(p.peek(1), "STATEMENT"):
			if !p.skipSetStatementVars() {
				return stmts, nil
			}
		default:
			// every other head (CREATE, DROP, INSERT, GRANT, XA, BEGIN, ...) ignores
			// the ENTIRE remaining input — routine bodies contain ';', so no splitting
			return stmts, nil
		}
	}
}

func (p *ddlParser) peek(i int) ddlToken {
	for len(p.buf) <= i {
		if p.lexErr != nil {
			return ddlToken{kind: tokErr, pos: p.lx.pos}
		}
		t, err := p.lx.next()
		if err != nil {
			p.lexErr = err
			return ddlToken{kind: tokErr, pos: p.lx.pos}
		}
		p.buf = append(p.buf, t)
	}
	return p.buf[i]
}

func (p *ddlParser) next() {
	p.peek(0)
	if len(p.buf) > 0 {
		p.buf = p.buf[1:]
	}
}

func (p *ddlParser) peekPunct(i int, c byte) bool {
	t := p.peek(i)
	return t.kind == tokPunct && t.text[0] == c
}

// consumeWords consumes the given keyword sequence when it appears next in full,
// reporting whether it did.
func (p *ddlParser) consumeWords(kws ...string) bool {
	for i, kw := range kws {
		if !ddlWordIs(p.peek(i), kw) {
			return false
		}
	}
	for range kws {
		p.next()
	}
	return true
}

func (p *ddlParser) expectIdent(what string) (string, error) {
	t, err := p.expectIdentToken(what)
	if err != nil {
		return "", err
	}
	return t.text, nil
}

func (p *ddlParser) expectIdentToken(what string) (ddlToken, error) {
	t := p.peek(0)
	if t.kind == tokWord || t.kind == tokQuotedIdent {
		p.next()
		return t, nil
	}
	if t.kind == tokErr {
		return ddlToken{}, p.lexErr
	}
	return ddlToken{}, fmt.Errorf("expected %s at byte %d", what, t.pos)
}

// parseTableIdent parses ident, ident.ident, or .ident (any quoting; after '.'
// even all-digit words are identifiers). Schema is empty when unqualified.
func (p *ddlParser) parseTableIdent() (string, string, error) {
	if p.peekPunct(0, '.') {
		p.next()
		table, err := p.expectIdentToken("table name after '.'")
		if err != nil {
			return "", "", err
		}
		return "", table.text, nil
	}
	first, err := p.expectIdentToken("table name")
	if err != nil {
		return "", "", err
	}
	if p.peekPunct(0, '.') {
		p.next()
		table, err := p.expectIdentToken("table name after '.'")
		if err != nil {
			return "", "", err
		}
		return first.text, table.text, nil
	}
	return "", first.text, nil
}

// skipLockWait consumes MariaDB's WAIT n | NOWAIT lock timeout clause.
func (p *ddlParser) skipLockWait() {
	if ddlWordIs(p.peek(0), "NOWAIT") {
		p.next()
		return
	}
	if !ddlWordIs(p.peek(0), "WAIT") {
		return
	}
	n := lockWaitTimeoutTokenCount(p)
	if n == 0 {
		return
	}
	p.next()
	for range n {
		p.next()
	}
}

func lockWaitTimeoutTokenCount(p *ddlParser) int {
	switch {
	case isUnsignedNumberWord(p.peek(1), true):
		return 1
	case p.peekPunct(1, '+') && isUnsignedNumberWord(p.peek(2), false):
		return 2
	case p.peekPunct(1, '.') && isAllDigits(p.peek(2)):
		return 2
	case p.peekPunct(1, '+') && p.peekPunct(2, '.') && isAllDigits(p.peek(3)):
		return 3
	default:
		return 0
	}
}

func isUnsignedNumberWord(t ddlToken, allowHex bool) bool {
	if t.kind != tokWord || t.text == "" {
		return false
	}
	s := t.text
	if len(s) > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		if !allowHex {
			return false
		}
		for i := 2; i < len(s); i++ {
			if !isDDLHexDigitByte(s[i]) {
				return false
			}
		}
		return true
	}
	i := 0
	for i < len(s) && isDDLDigitByte(s[i]) {
		i++
	}
	if i == 0 {
		return false
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && isDDLDigitByte(s[i]) {
			i++
		}
	}
	if end, ok := scanDDLExponent(s, i); ok {
		i = end
	}
	return i == len(s)
}

func isAllDigits(t ddlToken) bool {
	if t.kind != tokWord || t.text == "" {
		return false
	}
	for i := range len(t.text) {
		if !isDDLDigitByte(t.text[i]) {
			return false
		}
	}
	return true
}

// skipSetStatementVars consumes MariaDB's "SET STATEMENT var=val[, ...] FOR" so
// the wrapped statement can be classified. Values are full expressions — strings
// may contain FOR — so the FOR keyword is only recognized at paren depth 0.
// Returns false when no such FOR exists; the statement is then ignored.
func (p *ddlParser) skipSetStatementVars() bool {
	p.next() // SET
	p.next() // STATEMENT
	depth := 0
	for {
		t := p.peek(0)
		switch {
		case t.kind == tokEOF || t.kind == tokErr:
			return false
		case depth == 0 && ddlWordIs(t, "FOR"):
			p.next()
			return true
		case t.kind == tokPunct && t.text == "(":
			depth++
			p.next()
		case t.kind == tokPunct && t.text == ")":
			if depth == 0 {
				return false
			}
			depth--
			p.next()
		default:
			p.next()
		}
	}
}

func (p *ddlParser) parseAlterTable() ([]ddlStatement, error) {
	p.next() // ALTER
	for ddlWordIs(p.peek(0), "ONLINE") || ddlWordIs(p.peek(0), "IGNORE") {
		p.next()
	}
	p.next() // TABLE
	p.consumeWords("IF", "EXISTS")
	schema, table, err := p.parseTableIdent()
	if err != nil {
		return nil, err
	}
	p.skipLockWait()
	stmt := &ddlAlterTable{Schema: schema, Table: table}
	knownColumns := make(map[string]struct{})
	var rename *ddlRenamePair
	partitionListMayContinue := false
	for {
		t := p.peek(0)
		if t.kind == tokEOF || (t.kind == tokPunct && t.text == ";") {
			return ddlAlterTableStatements(stmt, rename), nil
		}
		if t.kind == tokErr {
			return nil, p.lexErr
		}
		spec, pair, nextPartitionListMayContinue, err := p.parseAlterSpec(schema, table, partitionListMayContinue)
		if err != nil {
			return nil, err
		}
		partitionListMayContinue = nextPartitionListMayContinue
		if spec != nil {
			spec = ddlFilterAddIfNotExists(spec, knownColumns)
		}
		if spec != nil {
			ddlApplyAlterSpecColumnNames(knownColumns, spec)
			stmt.Specs = append(stmt.Specs, *spec)
		}
		if pair != nil {
			rename = pair
		}
		if spec != nil || pair != nil {
			if err := p.consumeAlterTableSpecTail(); err != nil {
				return nil, err
			}
		}
		t = p.peek(0)
		switch {
		case t.kind == tokPunct && t.text == ",":
			p.next()
		case t.kind == tokEOF || (t.kind == tokPunct && t.text == ";"):
			return ddlAlterTableStatements(stmt, rename), nil
		case t.kind == tokErr:
			return nil, p.lexErr
		default:
			return nil, fmt.Errorf("unexpected token %q after ALTER TABLE spec at byte %d", t.text, t.pos)
		}
	}
}

func ddlAlterTableStatements(alter *ddlAlterTable, rename *ddlRenamePair) []ddlStatement {
	if rename == nil || ddlRenamePairIsNoop(*rename) {
		return []ddlStatement{alter}
	}
	renameStmt := &ddlRenameTable{Pairs: []ddlRenamePair{*rename}}
	out := make([]ddlStatement, 0, 2)
	if len(alter.Specs) > 0 {
		out = append(out, alter)
	}
	out = append(out, renameStmt)
	return out
}

func ddlRenamePairIsNoop(pair ddlRenamePair) bool {
	return pair.OldSchema == pair.NewSchema && pair.OldTable == pair.NewTable
}

func ddlFilterAddIfNotExists(spec *ddlAlterSpec, knownColumns map[string]struct{}) *ddlAlterSpec {
	if !spec.AddIfNotExists {
		return spec
	}
	spec.AddIfNotExists = false
	cols := spec.NewColumns[:0]
	for _, col := range spec.NewColumns {
		if _, ok := knownColumns[ddlColumnNameKey(col.Name)]; ok {
			continue
		}
		cols = append(cols, col)
	}
	spec.NewColumns = cols
	if len(spec.NewColumns) == 0 && spec.OldColumnName == "" && !spec.RenameColumn {
		return nil
	}
	return spec
}

func ddlApplyAlterSpecColumnNames(knownColumns map[string]struct{}, spec *ddlAlterSpec) {
	var removes, adds []string
	switch {
	case spec.RenameColumn:
		removes = append(removes, spec.OldColumnName)
		adds = append(adds, spec.NewColumnName)
	case len(spec.NewColumns) > 0:
		if spec.ChangeColumn && len(spec.NewColumns) == 1 && spec.OldColumnName != spec.NewColumns[0].Name {
			removes = append(removes, spec.OldColumnName)
		}
		for _, col := range spec.NewColumns {
			adds = append(adds, col.Name)
		}
	case spec.OldColumnName != "":
		removes = append(removes, spec.OldColumnName)
	}
	for _, name := range removes {
		delete(knownColumns, ddlColumnNameKey(name))
	}
	for _, name := range adds {
		knownColumns[ddlColumnNameKey(name)] = struct{}{}
	}
}

func ddlColumnNameKey(name string) string {
	return strings.ToLower(name)
}

func ddlAlterSpecsHaveImplicitPositionShift(specs []ddlAlterSpec) bool {
	removed := make(map[string]struct{})
	added := make(map[string]struct{})
	for _, spec := range specs {
		var removes, adds []string
		switch {
		case spec.RenameColumn:
			removes = append(removes, spec.OldColumnName)
			adds = append(adds, spec.NewColumnName)
		case len(spec.NewColumns) > 0:
			if spec.ChangeColumn && len(spec.NewColumns) == 1 && spec.OldColumnName != spec.NewColumns[0].Name {
				removes = append(removes, spec.OldColumnName)
			}
			for _, col := range spec.NewColumns {
				adds = append(adds, col.Name)
			}
		case spec.OldColumnName != "":
			removes = append(removes, spec.OldColumnName)
		}
		for _, name := range removes {
			if _, ok := added[name]; ok {
				return true
			}
		}
		for _, name := range adds {
			if _, ok := removed[name]; ok {
				return true
			}
		}
		for _, name := range removes {
			removed[name] = struct{}{}
		}
		for _, name := range adds {
			added[name] = struct{}{}
		}
	}
	return false
}

// parseAlterSpec parses one comma-separated ALTER TABLE spec. Column-relevant
// specs are returned; everything else — index/constraint/option/partition specs,
// ALGORITHM/LOCK/FORCE/ORDER BY, CONVERT TO, table options — is consumed
// (balanced parens, quote-aware) and dropped by returning nil.
func (p *ddlParser) parseAlterSpec(tableSchema, table string, partitionListMayContinue bool) (*ddlAlterSpec, *ddlRenamePair, bool, error) {
	if partitionListMayContinue && p.consumePartitionNameListContinuation() {
		return nil, nil, true, nil
	}
	t := p.peek(0)
	if t.kind != tokWord {
		return nil, nil, false, p.skipSpecRemainder()
	}
	switch {
	case ddlWordIs(t, "ADD"):
		p.next()
		spec, err := p.parseAddSpec()
		return spec, nil, false, err
	case ddlWordIs(t, "DROP"):
		p.next()
		spec, continuesPartitionList, err := p.parseDropSpec()
		return spec, nil, continuesPartitionList, err
	case ddlWordIs(t, "MODIFY"):
		p.next()
		spec, err := p.parseModifySpec()
		return spec, nil, false, err
	case ddlWordIs(t, "CHANGE"):
		p.next()
		spec, err := p.parseChangeSpec()
		return spec, nil, false, err
	case ddlWordIs(t, "RENAME"):
		p.next()
		spec, pair, err := p.parseRenameSpec(tableSchema, table)
		return spec, pair, false, err
	case ddlWordIs(t, "ORDER") && ddlWordIs(p.peek(1), "BY"):
		p.next()
		p.next()
		return nil, nil, false, p.skipAlterOrderList()
	case ddlAlterSpecStartsPartitionNameList(t, p.peek(1)):
		return nil, nil, true, p.skipSpecRemainder()
	default:
		return nil, nil, false, p.skipSpecRemainder()
	}
}

func ddlAlterSpecStartsPartitionNameList(t, next ddlToken) bool {
	if !ddlWordIs(next, "PARTITION") {
		return false
	}
	switch strings.ToUpper(t.text) {
	case "REBUILD", "OPTIMIZE", "ANALYZE", "CHECK", "REPAIR", "TRUNCATE",
		"DISCARD", "IMPORT", "REORGANIZE":
		return true
	default:
		return false
	}
}

func (p *ddlParser) consumePartitionNameListContinuation() bool {
	t := p.peek(0)
	if t.kind != tokWord && t.kind != tokQuotedIdent {
		return false
	}
	next := p.peek(1)
	if next.kind == tokEOF || (next.kind == tokPunct && (next.text == "," || next.text == ";")) {
		p.next()
		return true
	}
	return false
}

func (p *ddlParser) parseAddSpec() (*ddlAlterSpec, error) {
	sawColumn := p.consumeWords("COLUMN")
	sawIfNotExists := p.consumeWords("IF", "NOT", "EXISTS")
	if p.peekPunct(0, '(') {
		spec := &ddlAlterSpec{AddIfNotExists: sawIfNotExists}
		if err := p.parseColumnList(spec); err != nil {
			return nil, err
		}
		if len(spec.NewColumns) == 0 {
			return nil, nil
		}
		return spec, nil
	}
	// bare ADD needs disambiguation; an explicit COLUMN or IF NOT EXISTS already
	// commits to the column branch (ADD IF NOT EXISTS period date is a real
	// MariaDB column named period)
	if !sawColumn && !sawIfNotExists {
		if t := p.peek(0); t.kind == tokWord {
			switch strings.ToUpper(t.text) {
			case "INDEX", "KEY", "FULLTEXT", "SPATIAL", "UNIQUE", "PRIMARY", "FOREIGN",
				"CONSTRAINT", "CHECK", "PARTITION":
				return nil, p.skipSpecRemainder()
			// PERIOD, SYSTEM and VECTOR are valid MySQL column names; only the
			// two-word forms are benign operations, so peek one more word
			case "PERIOD":
				if ddlWordIs(p.peek(1), "FOR") || ddlWordIs(p.peek(1), "IF") {
					return nil, p.skipSpecRemainder()
				}
			case "SYSTEM":
				if ddlWordIs(p.peek(1), "VERSIONING") {
					return nil, p.skipSpecRemainder()
				}
			case "VECTOR":
				// MariaDB's key_def makes INDEX/KEY and the index name optional
				// after VECTOR (sql_yacc.yy spatial_or_vector opt_key_or_index
				// opt_if_not_exists opt_ident), so ADD VECTOR v(c) is a
				// vector-index add there, not a column.
				if ddlWordIs(p.peek(1), "INDEX") || ddlWordIs(p.peek(1), "KEY") || ddlWordIs(p.peek(1), "IF") ||
					p.looksLikeMariaVectorIndexAdd() {
					return nil, p.skipSpecRemainder()
				}
			}
		}
	}
	name, err := p.expectIdent("column name")
	if err != nil {
		return nil, err
	}
	spec := &ddlAlterSpec{}
	if err := p.parseColumnDef(name, spec, false); err != nil {
		return nil, err
	}
	spec.AddIfNotExists = sawIfNotExists
	return spec, nil
}

func (p *ddlParser) looksLikeMariaVectorIndexAdd() bool {
	if !p.lx.isMariaDB {
		return false
	}
	i := 1 // token after VECTOR
	if ddlWordIs(p.peek(i), "INDEX") || ddlWordIs(p.peek(i), "KEY") {
		i++
	}
	if ddlWordIs(p.peek(i), "IF") && ddlWordIs(p.peek(i+1), "NOT") && ddlWordIs(p.peek(i+2), "EXISTS") {
		i += 3
	}
	if p.peekPunct(i, '(') {
		return true
	}
	t := p.peek(i)
	if t.kind == tokWord || t.kind == tokQuotedIdent {
		return p.peekPunct(i+1, '(')
	}
	return false
}

func (p *ddlParser) parseDropSpec() (*ddlAlterSpec, bool, error) {
	sawColumn := p.consumeWords("COLUMN")
	sawIfExists := p.consumeWords("IF", "EXISTS")
	if !sawColumn && !sawIfExists {
		if t := p.peek(0); t.kind == tokWord {
			switch strings.ToUpper(t.text) {
			case "INDEX", "KEY", "PRIMARY", "FOREIGN", "CONSTRAINT", "CHECK", "PARTITION":
				return nil, ddlWordIs(t, "PARTITION"), p.skipSpecRemainder()
			case "PERIOD":
				if ddlWordIs(p.peek(1), "FOR") || ddlWordIs(p.peek(1), "IF") {
					return nil, false, p.skipSpecRemainder()
				}
			case "SYSTEM":
				if ddlWordIs(p.peek(1), "VERSIONING") {
					return nil, false, p.skipSpecRemainder()
				}
			case "VECTOR":
				if ddlWordIs(p.peek(1), "INDEX") || ddlWordIs(p.peek(1), "KEY") {
					return nil, false, p.skipSpecRemainder()
				}
			}
		}
	}
	// any other word after DROP is a column name (DROP first, DROP algorithm, ...)
	name, err := p.expectIdent("column name")
	if err != nil {
		return nil, false, err
	}
	// RESTRICT | CASCADE (MariaDB)
	return &ddlAlterSpec{OldColumnName: name}, false, p.skipSpecRemainder()
}

func (p *ddlParser) parseModifySpec() (*ddlAlterSpec, error) {
	p.consumeWords("COLUMN")
	sawIfExists := p.consumeWords("IF", "EXISTS")
	if t := p.peek(0); t.kind == tokPunct && t.text == "," {
		return nil, nil
	}
	name, err := p.expectIdent("column name")
	if err != nil {
		return nil, err
	}
	spec := &ddlAlterSpec{ModifyIfExists: sawIfExists}
	if err := p.parseColumnDef(name, spec, false); err != nil {
		return nil, err
	}
	return spec, nil
}

func (p *ddlParser) parseChangeSpec() (*ddlAlterSpec, error) {
	p.consumeWords("COLUMN")
	p.consumeWords("IF", "EXISTS")
	oldName, err := p.expectIdent("column name")
	if err != nil {
		return nil, err
	}
	newName, err := p.expectIdent("new column name")
	if err != nil {
		return nil, err
	}
	spec := &ddlAlterSpec{OldColumnName: oldName, ChangeColumn: true}
	if err := p.parseColumnDef(newName, spec, false); err != nil {
		return nil, err
	}
	return spec, nil
}

func (p *ddlParser) parseRenameSpec(tableSchema, table string) (*ddlAlterSpec, *ddlRenamePair, error) {
	t := p.peek(0)
	switch {
	case ddlWordIs(t, "COLUMN"):
		p.next()
		p.consumeWords("IF", "EXISTS")
		oldName, err := p.expectIdent("column name")
		if err != nil {
			return nil, nil, err
		}
		if !p.consumeWords("TO") {
			at := p.peek(0)
			if at.kind == tokErr {
				return nil, nil, p.lexErr
			}
			return nil, nil, fmt.Errorf("expected TO in RENAME COLUMN at byte %d", at.pos)
		}
		newName, err := p.expectIdent("new column name")
		if err != nil {
			return nil, nil, err
		}
		return &ddlAlterSpec{OldColumnName: oldName, NewColumnName: newName, RenameColumn: true}, nil, nil
	case ddlWordIs(t, "INDEX") || ddlWordIs(t, "KEY"):
		return nil, nil, p.skipSpecRemainder()
	default:
		newSchema, newTable, err := p.parseAlterTableRenameTarget()
		if err != nil {
			return nil, nil, err
		}
		return nil, &ddlRenamePair{OldSchema: tableSchema, OldTable: table, NewSchema: newSchema, NewTable: newTable}, nil
	}
}

func (p *ddlParser) parseAlterTableRenameTarget() (string, string, error) {
	if ddlWordIs(p.peek(0), "TO") || ddlWordIs(p.peek(0), "AS") || p.peekPunct(0, '=') {
		p.next()
	}
	return p.parseTableIdent()
}

func (p *ddlParser) consumeAlterTableSpecTail() error {
	switch {
	case ddlWordIs(p.peek(0), "REMOVE") && ddlWordIs(p.peek(1), "PARTITIONING"):
		p.next()
		p.next()
		return nil
	case ddlWordIs(p.peek(0), "PARTITION") && ddlWordIs(p.peek(1), "BY"):
		return p.skipSpecRemainder()
	default:
		return nil
	}
}

// skipSpecRemainder consumes the rest of the current alter spec — until a ',' or
// ';' at paren depth 0, or end of input — keeping parens balanced and strings
// intact. A lexer error or unbalanced ')' here is a parse failure: benign specs
// must be consumed correctly, never guessed past.
func (p *ddlParser) skipSpecRemainder() error {
	for {
		t := p.peek(0)
		switch {
		case t.kind == tokErr:
			return p.lexErr
		case t.kind == tokEOF:
			return nil
		case t.kind == tokPunct && (t.text == "," || t.text == ";"):
			return nil
		case t.kind == tokPunct && t.text == "(":
			if err := p.skipBalanced(); err != nil {
				return err
			}
		case t.kind == tokPunct && t.text == ")":
			return fmt.Errorf("unbalanced ')' at byte %d", t.pos)
		default:
			p.next()
		}
	}
}

func (p *ddlParser) skipAlterOrderList() error {
	for {
		t := p.peek(0)
		switch {
		case t.kind == tokErr:
			return p.lexErr
		case t.kind == tokEOF:
			return nil
		case t.kind == tokPunct && t.text == ";":
			return nil
		case t.kind == tokPunct && t.text == ")":
			return fmt.Errorf("unbalanced ')' at byte %d", t.pos)
		default:
			p.next()
		}
	}
}

// skipBalanced consumes a '(' and everything through its matching ')'.
func (p *ddlParser) skipBalanced() error {
	p.next() // (
	depth := 1
	for depth > 0 {
		t := p.peek(0)
		switch {
		case t.kind == tokErr:
			return p.lexErr
		case t.kind == tokEOF:
			return errors.New("unterminated '(' group")
		case t.kind == tokPunct && t.text == "(":
			depth++
		case t.kind == tokPunct && t.text == ")":
			depth--
		}
		p.next()
	}
	return nil
}

// parseColumnList parses the parenthesized ADD (...) form, whose elements mix
// column definitions with index/constraint/CHECK elements; only columns are kept.
func (p *ddlParser) parseColumnList(spec *ddlAlterSpec) error {
	p.next() // (
	for {
		t := p.peek(0)
		switch t.kind {
		case tokErr:
			return p.lexErr
		case tokEOF:
			return errors.New("unterminated column list")
		case tokWord:
			benign := false
			switch strings.ToUpper(t.text) {
			case "INDEX", "KEY", "FULLTEXT", "SPATIAL", "UNIQUE", "PRIMARY", "FOREIGN",
				"CONSTRAINT", "CHECK":
				benign = true
			case "PERIOD":
				benign = ddlWordIs(p.peek(1), "FOR")
			case "VECTOR":
				benign = p.looksLikeMariaVectorIndexAdd()
			}
			if benign {
				if err := p.skipListElement(); err != nil {
					return err
				}
			} else {
				p.next()
				if err := p.parseColumnDef(t.text, spec, true); err != nil {
					return err
				}
			}
		case tokQuotedIdent:
			p.next()
			if err := p.parseColumnDef(t.text, spec, true); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected token in column list at byte %d", t.pos)
		}
		t = p.peek(0)
		switch {
		case t.kind == tokPunct && t.text == ",":
			p.next()
		case t.kind == tokPunct && t.text == ")":
			p.next()
			return nil
		case t.kind == tokErr:
			return p.lexErr
		default:
			return fmt.Errorf("unexpected token in column list at byte %d", t.pos)
		}
	}
}

// skipListElement consumes an index/constraint element of an ADD (...) list up
// to the ',' or ')' that ends it, without consuming the terminator.
func (p *ddlParser) skipListElement() error {
	for {
		t := p.peek(0)
		switch {
		case t.kind == tokErr:
			return p.lexErr
		case t.kind == tokEOF:
			return errors.New("unterminated column list")
		case t.kind == tokPunct && (t.text == "," || t.text == ")"):
			return nil
		case t.kind == tokPunct && t.text == "(":
			if err := p.skipBalanced(); err != nil {
				return err
			}
		default:
			p.next()
		}
	}
}

// ddlColumnState carries a column definition through type and attribute parsing
// before it is rendered into a ddlColumnDef.
type ddlColumnState struct {
	base          string
	enumList      string   // raw enum/set element list exactly as written
	params        []string // written type params
	written       bool     // a '(' params list was written in the DDL text
	synthetic     bool     // params were synthesized (BOOLEAN -> tinyint(1)), not written
	unsigned      bool
	zerofill      bool
	notNull       bool
	charsetSeen   bool
	charsetBinary bool
}

// ddlBinaryCharsetRemap: CHARACTER SET binary (or MariaDB's BYTE suffix) turns
// character types into their binary counterparts. The bare BINARY attribute
// keyword is a collation marker and does NOT remap.
var ddlBinaryCharsetRemap = map[string]string{
	"char":       "binary",
	"varchar":    "varbinary",
	"tinytext":   "tinyblob",
	"text":       "blob",
	"mediumtext": "mediumblob",
	"longtext":   "longblob",
}

var ddlCharacterCharsetRemap = map[string]string{
	"binary":     "char",
	"varbinary":  "varchar",
	"tinyblob":   "tinytext",
	"blob":       "text",
	"mediumblob": "mediumtext",
	"longblob":   "longtext",
}

func ddlRemapToBinary(st *ddlColumnState) {
	if remapped, ok := ddlBinaryCharsetRemap[st.base]; ok {
		st.base = remapped
	}
}

func ddlRemapToCharacter(st *ddlColumnState) {
	if remapped, ok := ddlCharacterCharsetRemap[st.base]; ok {
		st.base = remapped
	}
}

// Precision/Scale feed numeric typmods downstream; only decimal/numeric params
// and integer display widths are consumed there, everything else stays -1.
func ddlNumericParamsApply(base string) bool {
	switch base {
	case "decimal", "numeric", "tinyint", "smallint", "mediumint", "int", "bigint":
		return true
	default:
		return false
	}
}

func ddlSpatialType(base string) bool {
	switch base {
	case "geometry", "point", "linestring", "polygon", "multipoint",
		"multilinestring", "multipolygon", "geometrycollection", "geomcollection":
		return true
	default:
		return false
	}
}

// parseColumnDef parses "<type> <attributes...>" for the named column, appending
// the result to spec.NewColumns. insideParens is true within the ADD (...) form,
// where an unconsumed ')' ends the definition.
func (p *ddlParser) parseColumnDef(name string, spec *ddlAlterSpec, insideParens bool) error {
	var st ddlColumnState
	if err := p.parseDataType(&st); err != nil {
		return err
	}
	if err := p.parseColumnAttributes(&st, spec, insideParens); err != nil {
		return err
	}
	ddlNormalizeMySQLUnsignedTinyintOne(&st, p.lx.isMariaDB)
	precision, scale := -1, -1
	if st.written && !st.synthetic && ddlNumericParamsApply(st.base) {
		if len(st.params) >= 1 {
			precision = digitsToInt(st.params[0])
		}
		if len(st.params) >= 2 {
			scale = digitsToInt(st.params[1])
		}
	}
	typeStr := st.base
	if st.written {
		if st.base == "enum" || st.base == "set" {
			typeStr += "(" + st.enumList + ")"
		} else {
			typeStr += "(" + strings.Join(st.params, ",") + ")"
		}
	}
	if st.unsigned && st.base != "year" {
		typeStr += " unsigned"
	}
	spec.NewColumns = append(spec.NewColumns, ddlColumnDef{
		Name:      name,
		TypeStr:   typeStr,
		Precision: precision,
		Scale:     scale,
		NotNull:   st.notNull,
	})
	return nil
}

func digitsToInt(s string) int {
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return -1
	}
	return v
}

func ddlNormalizeDecimalType(st *ddlColumnState) {
	if st.base != "decimal" && st.base != "numeric" {
		return
	}
	if !st.written || st.synthetic || len(st.params) == 0 {
		return
	}
	if digitsToInt(st.params[0]) == 0 {
		st.base = "decimal"
		st.params = []string{"10", "0"}
	}
}

func ddlNormalizeFloatPrecisionType(st *ddlColumnState) {
	if st.base != "float" || !st.written || st.synthetic || len(st.params) != 1 {
		return
	}
	if digitsToInt(st.params[0]) > 24 {
		st.base = "double"
		st.params = nil
		st.written = false
	}
}

func ddlNormalizeIntegerDisplayWidth(st *ddlColumnState) {
	if !st.written || st.synthetic || len(st.params) == 0 {
		return
	}
	switch st.base {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
	default:
		return
	}
	if width := digitsToInt(st.params[0]); width >= 0 {
		st.params[0] = strconv.Itoa(width)
	}
}

func ddlNormalizeMySQLUnsignedTinyintOne(st *ddlColumnState, isMariaDB bool) {
	if isMariaDB || !st.unsigned || st.base != "tinyint" || !st.written || len(st.params) != 1 {
		return
	}
	if digitsToInt(st.params[0]) == 1 {
		st.params = nil
		st.written = false
	}
}

// udtType resolves an unrecognized word in type position. MariaDB treats any
// identifier there as a candidate UDT (uuid, inet4/6, vector, geometry family,
// plugin types); MySQL's type keyword set is closed, so an unknown word is a
// parse error — the safe direction, reported rather than silently dropped.
func (p *ddlParser) udtType(st *ddlColumnState, t ddlToken) error {
	if !p.lx.isMariaDB {
		return fmt.Errorf("unknown data type %q at byte %d", t.text, t.pos)
	}
	st.base = strings.ToLower(t.text)
	return nil
}

// oracleOrUDTType resolves an Oracle-mode type name: with sql_mode=ORACLE it maps
// to oracleBase, otherwise it falls back to the MariaDB UDT rule / MySQL error.
func (p *ddlParser) oracleOrUDTType(st *ddlColumnState, t ddlToken, oracle bool, oracleBase string) error {
	if !oracle {
		return p.udtType(st, t)
	}
	st.base = oracleBase
	return nil
}

func (p *ddlParser) parseDataType(st *ddlColumnState) error {
	oracle := p.lx.isMariaDB && p.lx.sqlMode&sqlModeOracle != 0
	// MariaDB schema-qualified types: mariadb_schema.DATE and friends. The
	// qualifier picks the type-mapping schema regardless of session sql_mode
	// (sql_schema.cc: oracle_schema DATE->datetime, maxdb_schema
	// TIMESTAMP->datetime); without a qualifier ORACLE mode implies oracle_schema.
	var typeSchema string
	if p.lx.isMariaDB && p.peek(0).kind == tokWord && p.peekPunct(1, '.') && p.peek(2).kind == tokWord {
		typeSchema = strings.ToLower(p.peek(0).text)
		p.next()
		p.next()
	}
	t := p.peek(0)
	if t.kind != tokWord && t.kind != tokQuotedIdent {
		if t.kind == tokErr {
			return p.lexErr
		}
		return fmt.Errorf("expected data type at byte %d", t.pos)
	}
	p.next()
	w := strings.ToLower(t.text)
	switch w {
	case "national":
		nt := p.peek(0)
		switch {
		// VARCHARACTER is a lexer-level alias of VARCHAR in both servers
		case ddlWordIs(nt, "VARCHAR") || ddlWordIs(nt, "VARCHARACTER"):
			p.next()
			st.base = "varchar"
		case ddlWordIs(nt, "CHAR") || ddlWordIs(nt, "CHARACTER"):
			p.next()
			st.base = "char"
			if ddlWordIs(p.peek(0), "VARYING") {
				p.next()
				st.base = "varchar"
			}
		default:
			return fmt.Errorf("expected CHAR or VARCHAR after NATIONAL at byte %d", nt.pos)
		}
	case "nchar":
		st.base = "char"
		if ddlWordIs(p.peek(0), "VARCHAR") || ddlWordIs(p.peek(0), "VARCHARACTER") || ddlWordIs(p.peek(0), "VARYING") {
			p.next()
			st.base = "varchar"
		}
	case "nvarchar":
		st.base = "varchar"
	case "char", "character":
		st.base = "char"
		if ddlWordIs(p.peek(0), "VARYING") {
			p.next()
			st.base = "varchar"
		}
	case "varchar", "varcharacter":
		st.base = "varchar"
	case "long":
		switch {
		case ddlWordIs(p.peek(0), "VARBINARY"):
			p.next()
			st.base = "mediumblob"
		case ddlWordIs(p.peek(0), "VARCHAR") || ddlWordIs(p.peek(0), "VARCHARACTER"):
			p.next()
			st.base = "mediumtext"
		default:
			st.base = "mediumtext"
		}
	case "double":
		st.base = "double"
		if ddlWordIs(p.peek(0), "PRECISION") {
			p.next()
		}
	case "real", "float8":
		st.base = "double"
	case "float", "float4":
		st.base = "float"
	case "int1", "tinyint":
		st.base = "tinyint"
	case "int2", "smallint":
		st.base = "smallint"
	case "int3", "middleint", "mediumint":
		st.base = "mediumint"
	case "int4", "integer", "int":
		st.base = "int"
	case "int8", "bigint":
		st.base = "bigint"
	case "dec", "fixed", "decimal":
		st.base = "decimal"
	case "numeric":
		st.base = "numeric"
	case "bool", "boolean":
		st.base = "tinyint"
		st.params = []string{"1"}
		st.written = true
		st.synthetic = true
	case "serial":
		st.base = "bigint"
		st.unsigned = true
		st.notNull = true
	case "clob":
		st.base = "longtext"
	case "varchar2":
		if err := p.oracleOrUDTType(st, t, oracle, "varchar"); err != nil {
			return err
		}
	case "raw":
		if err := p.oracleOrUDTType(st, t, oracle, "varbinary"); err != nil {
			return err
		}
	case "number":
		// resolved to decimal or double once params are known
		if err := p.oracleOrUDTType(st, t, oracle, "number"); err != nil {
			return err
		}
	case "json":
		// MariaDB has no native JSON type: the MariaDB KB documents JSON as an
		// alias for LONGTEXT, and information_schema reports longtext. MySQL
		// keeps the native type.
		if p.lx.isMariaDB {
			st.base = "longtext"
		} else {
			st.base = "json"
		}
	case "sql_tsi_year", "year":
		st.base = "year"
	case "bit", "date", "time", "datetime", "timestamp",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
		"tinytext", "text", "mediumtext", "longtext", "enum", "set",
		"geometry", "point", "linestring", "polygon", "multipoint",
		"multilinestring", "multipolygon", "geometrycollection", "geomcollection", "vector":
		st.base = w
	default:
		if err := p.udtType(st, t); err != nil {
			return err
		}
	}
	if p.peekPunct(0, '(') && !st.synthetic && !ddlSpatialType(st.base) {
		paramsWritten := true
		if st.base == "enum" || st.base == "set" {
			raw, err := p.captureParenContents()
			if err != nil {
				return err
			}
			st.enumList = raw
		} else if st.base == "vector" {
			params, written, err := p.parseVectorTypeParams()
			if err != nil {
				return err
			}
			st.params = params
			paramsWritten = written
		} else {
			params, err := p.parseTypeParams()
			if err != nil {
				return err
			}
			st.params = params
		}
		st.written = paramsWritten
	}
	if oracle {
		switch {
		case st.base == "number" && st.written:
			st.base = "decimal"
		case st.base == "number":
			st.base = "double"
		case w == "blob" && !st.written:
			st.base = "longblob"
		}
	}
	switch {
	case (typeSchema == "oracle_schema" || (typeSchema == "" && oracle)) && st.base == "date":
		st.base = "datetime"
	case typeSchema == "maxdb_schema" && st.base == "timestamp":
		st.base = "datetime"
	}
	ddlNormalizeFloatPrecisionType(st)
	ddlNormalizeDecimalType(st)
	ddlNormalizeIntegerDisplayWidth(st)
	return nil
}

func (p *ddlParser) parseVectorTypeParams() ([]string, bool, error) {
	if !p.vectorTypeParamsAreSimple() {
		_, err := p.captureParenContents()
		return nil, false, err
	}
	params, err := p.parseTypeParams()
	return params, true, err
}

func (p *ddlParser) vectorTypeParamsAreSimple() bool {
	i := 1
	if p.peek(i).kind != tokWord {
		return false
	}
	for {
		i++
		switch {
		case p.peekPunct(i, ')'):
			return true
		case p.peekPunct(i, ','):
			i++
			if p.peek(i).kind != tokWord {
				return false
			}
		default:
			return false
		}
	}
}

// captureParenContents consumes a balanced '(' ... ')' group and returns the raw
// source between the parens exactly as written; enum/set element lists pass
// through verbatim since downstream only looks at the base name before '('.
func (p *ddlParser) captureParenContents() (string, error) {
	open := p.peek(0)
	p.next() // (
	depth := 1
	for {
		t := p.peek(0)
		switch {
		case t.kind == tokErr:
			return "", p.lexErr
		case t.kind == tokEOF:
			return "", errors.New("unterminated type parameter list")
		case t.kind == tokPunct && t.text == "(":
			depth++
		case t.kind == tokPunct && t.text == ")":
			depth--
			if depth == 0 {
				p.next()
				return p.lx.s[open.pos+1 : t.pos], nil
			}
		}
		p.next()
	}
}

func (p *ddlParser) parseTypeParams() ([]string, error) {
	p.next() // (
	var params []string
	for {
		t := p.peek(0)
		if t.kind != tokWord {
			if t.kind == tokErr {
				return nil, p.lexErr
			}
			return nil, fmt.Errorf("expected type parameter at byte %d", t.pos)
		}
		p.next()
		params = append(params, t.text)
		t = p.peek(0)
		switch {
		case t.kind == tokPunct && t.text == ",":
			p.next()
		case t.kind == tokPunct && t.text == ")":
			p.next()
			return params, nil
		case t.kind == tokErr:
			return nil, p.lexErr
		default:
			return nil, fmt.Errorf("expected ',' or ')' in type parameters at byte %d", t.pos)
		}
	}
}

// parseColumnAttributes consumes everything after the data type until the end of
// the spec (',' or ';' at paren depth 0, an unconsumed ')' when insideParens, or
// end of input), extracting NOT NULL, FIRST/AFTER placement, UNSIGNED/ZEROFILL
// and charset-driven binary remaps; every other attribute is skipped.
func (p *ddlParser) parseColumnAttributes(st *ddlColumnState, spec *ddlAlterSpec, insideParens bool) error {
	for {
		t := p.peek(0)
		switch t.kind {
		case tokErr:
			return p.lexErr
		case tokEOF:
			if insideParens {
				return errors.New("unterminated column definition")
			}
			return nil
		case tokPunct:
			switch t.text {
			case ",", ";":
				return nil
			case ")":
				if insideParens {
					return nil
				}
				return fmt.Errorf("unbalanced ')' at byte %d", t.pos)
			case "(":
				if err := p.skipBalanced(); err != nil {
					return err
				}
			default:
				p.next()
			}
		case tokWord:
			if err := p.parseColumnAttributeWord(st, spec, t); err != nil {
				return err
			}
		default:
			p.next()
		}
	}
}

func (p *ddlParser) parseColumnAttributeWord(st *ddlColumnState, spec *ddlAlterSpec, t ddlToken) error {
	switch {
	case p.peekPunct(1, '='):
		// MariaDB ident=value engine options, REF_SYSTEM_ID=n, COMPRESSED=method,
		// ENGINE_ATTRIBUTE='json': consume the pair atomically so a keyword-shaped
		// value can never be misread as an attribute
		p.next()
		p.next()
		if v := p.peek(0); v.kind == tokWord || v.kind == tokString || v.kind == tokQuotedIdent {
			p.next()
		}
	case ddlWordIs(t, "DEFAULT"):
		p.next()
		return p.skipColumnAttributeValue()
	case ddlWordIs(t, "ON") && ddlWordIs(p.peek(1), "UPDATE"):
		p.next()
		p.next()
		return p.skipColumnAttributeValue()
	case ddlWordIs(t, "REFERENCES"):
		return p.skipColumnReference()
	case ddlWordIs(t, "COLUMN_FORMAT") || ddlWordIs(t, "STORAGE"):
		p.next()
		if v := p.peek(0); v.kind == tokWord || v.kind == tokString || v.kind == tokQuotedIdent {
			p.next()
		} else if v.kind == tokErr {
			return p.lexErr
		}
	case ddlWordIs(t, "NOT") && ddlWordIs(p.peek(1), "NULL"):
		st.notNull = true
		p.next()
		p.next()
	case ddlWordIs(t, "NULL"):
		st.notNull = false
		p.next()
	case ddlWordIs(t, "AUTO_INCREMENT"):
		st.notNull = true
		p.next()
	case ddlWordIs(t, "PRIMARY") && ddlWordIs(p.peek(1), "KEY"):
		st.notNull = true
		p.next()
		p.next()
	case ddlWordIs(t, "UNIQUE"):
		p.next()
		if ddlWordIs(p.peek(0), "KEY") {
			p.next()
		}
	case ddlWordIs(t, "KEY"):
		st.notNull = true
		p.next()
	case ddlWordIs(t, "UNSIGNED") || ddlWordIs(t, "ZEROFILL"):
		st.unsigned = true // zerofill implies unsigned
		if ddlWordIs(t, "ZEROFILL") {
			st.zerofill = true
		}
		p.next()
	case ddlWordIs(t, "FIRST"):
		spec.HasPosition = true
		p.next()
	case ddlWordIs(t, "AFTER"):
		spec.HasPosition = true
		p.next()
		// the placement argument can be any word, including keywords (AFTER first)
		if a := p.peek(0); a.kind == tokWord || a.kind == tokQuotedIdent {
			p.next()
		} else if a.kind == tokErr {
			return p.lexErr
		} else {
			return fmt.Errorf("expected column name after AFTER at byte %d", a.pos)
		}
	case ddlWordIs(t, "CHARSET"):
		p.next()
		p.applyCharset(st)
	case (ddlWordIs(t, "CHARACTER") || ddlWordIs(t, "CHAR")) && ddlWordIs(p.peek(1), "SET"):
		p.next()
		p.next()
		p.applyCharset(st)
	case ddlWordIs(t, "COLLATE"):
		p.next()
		p.applyCollation(st)
	case ddlWordIs(t, "BYTE"):
		// CHAR(n) BYTE et al: BYTE in the charset-suffix slot means binary on
		// both engines (mysql sql_yacc.yy opt_charset_with_opt_binary BYTE_SYM)
		ddlRemapToBinary(st)
		p.next()
	case ddlWordIs(t, "SERIAL") && ddlWordIs(p.peek(1), "DEFAULT") && ddlWordIs(p.peek(2), "VALUE"):
		// SERIAL DEFAULT VALUE applies NOT_NULL_FLAG (with auto_increment+unique)
		st.notNull = true
		p.next()
		p.next()
		p.next()
	default:
		p.next()
	}
	return nil
}

func (p *ddlParser) skipColumnAttributeValue() error {
	t := p.peek(0)
	switch {
	case t.kind == tokErr:
		return p.lexErr
	case t.kind == tokPunct && t.text == "(":
		return p.skipBalanced()
	case t.kind == tokPunct && (t.text == "+" || t.text == "-"):
		p.next()
		t = p.peek(0)
		if t.kind == tokErr {
			return p.lexErr
		}
	}
	if t.kind == tokWord || t.kind == tokString || t.kind == tokQuotedIdent {
		p.next()
		if p.peekPunct(0, '(') {
			return p.skipBalanced()
		}
	}
	return nil
}

func (p *ddlParser) skipColumnReference() error {
	p.next() // REFERENCES
	if t := p.peek(0); t.kind == tokWord || t.kind == tokQuotedIdent {
		p.next()
		if p.peekPunct(0, '.') {
			p.next()
			if t := p.peek(0); t.kind == tokWord || t.kind == tokQuotedIdent {
				p.next()
			} else if t.kind == tokErr {
				return p.lexErr
			}
		}
	} else if t.kind == tokErr {
		return p.lexErr
	}
	if p.peekPunct(0, '(') {
		if err := p.skipBalanced(); err != nil {
			return err
		}
	}
	for {
		switch {
		case ddlWordIs(p.peek(0), "MATCH"):
			p.next()
			if t := p.peek(0); t.kind == tokWord {
				p.next()
			} else if t.kind == tokErr {
				return p.lexErr
			}
		case ddlWordIs(p.peek(0), "ON") && (ddlWordIs(p.peek(1), "DELETE") || ddlWordIs(p.peek(1), "UPDATE")):
			p.next()
			p.next()
			if err := p.skipReferenceOption(); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

func (p *ddlParser) skipReferenceOption() error {
	switch {
	case ddlWordIs(p.peek(0), "SET") && (ddlWordIs(p.peek(1), "NULL") || ddlWordIs(p.peek(1), "DEFAULT")):
		p.next()
		p.next()
	case ddlWordIs(p.peek(0), "NO") && ddlWordIs(p.peek(1), "ACTION"):
		p.next()
		p.next()
	case p.peek(0).kind == tokWord:
		p.next()
	case p.peek(0).kind == tokErr:
		return p.lexErr
	}
	return nil
}

// applyCharset consumes a charset name; CHARACTER SET binary remaps character
// types to their binary counterparts.
// Non-binary charsets reverse binary string aliases back to character names.
func (p *ddlParser) applyCharset(st *ddlColumnState) {
	t := p.peek(0)
	if t.kind == tokWord || t.kind == tokQuotedIdent || t.kind == tokString {
		st.charsetSeen = true
		st.charsetBinary = strings.EqualFold(t.text, "binary")
		if st.charsetBinary {
			ddlRemapToBinary(st)
		} else {
			ddlRemapToCharacter(st)
		}
		p.next()
	}
}

func (p *ddlParser) applyCollation(st *ddlColumnState) {
	t := p.peek(0)
	if t.kind == tokWord || t.kind == tokQuotedIdent || t.kind == tokString {
		isBinary := strings.EqualFold(t.text, "binary")
		switch {
		case isBinary && (!st.charsetSeen || st.charsetBinary):
			ddlRemapToBinary(st)
		case !isBinary:
			ddlRemapToCharacter(st)
		}
		st.charsetSeen = true
		st.charsetBinary = isBinary
		p.next()
	}
}

func (p *ddlParser) parseRenameTable() (*ddlRenameTable, error) {
	p.next() // RENAME
	p.next() // TABLE or TABLES
	p.consumeWords("IF", "EXISTS")
	stmt := &ddlRenameTable{}
	for {
		oldSchema, oldTable, err := p.parseTableIdent()
		if err != nil {
			return nil, err
		}
		p.skipLockWait()
		if !p.consumeWords("TO") {
			t := p.peek(0)
			if t.kind == tokErr {
				return nil, p.lexErr
			}
			return nil, fmt.Errorf("expected TO in RENAME TABLE at byte %d", t.pos)
		}
		newSchema, newTable, err := p.parseTableIdent()
		if err != nil {
			return nil, err
		}
		pair := ddlRenamePair{
			OldSchema: oldSchema, OldTable: oldTable, NewSchema: newSchema, NewTable: newTable,
		}
		stmt.Pairs = append(stmt.Pairs, pair)
		t := p.peek(0)
		switch {
		case t.kind == tokPunct && t.text == ",":
			p.next()
		case t.kind == tokEOF || (t.kind == tokPunct && t.text == ";"):
			return stmt, nil
		case t.kind == tokErr:
			return nil, p.lexErr
		default:
			return nil, fmt.Errorf("unexpected token %q after RENAME TABLE pair at byte %d", t.text, t.pos)
		}
	}
}
