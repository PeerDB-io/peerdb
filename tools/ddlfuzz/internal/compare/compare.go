package compare

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

const (
	SQLModeANSIQuotes         uint64 = 1 << 2
	SQLModeOracle             uint64 = 1 << 9
	SQLModeMSSQL              uint64 = 1 << 10
	SQLModeNoBackslashEscapes uint64 = 1 << 20
)

const descriptorMask = SQLModeANSIQuotes | SQLModeOracle | SQLModeMSSQL | SQLModeNoBackslashEscapes

type Divergence struct {
	Class     string
	Shape     string
	OurSig    string
	OracleSig string
	OurError  string
	Skip      bool
}

type Descriptor struct {
	V       int    `json:"v"`
	Engine  string `json:"engine"`
	SQLMode uint64 `json:"sql_mode"`
	Class   string `json:"class"`
	Lane    string `json:"lane"`
	Shape   string `json:"shape"`
}

func OracleSig(d *digest.Digest) (sig string, skip bool) {
	if d == nil || d.Verdict != "accept" {
		return "", false
	}
	if len(d.Stmts) > 1 && d.Stmts[0].Kind == "other" {
		return "", true
	}
	stmts := d.Stmts
	for i, st := range stmts {
		if st.Kind == "other" {
			stmts = stmts[:i]
			break
		}
	}
	parts := make([]string, 0, len(stmts))
	for _, st := range stmts {
		switch st.Kind {
		case "alter_table":
			specs := make([]string, 0, len(st.Specs))
			for _, sp := range st.Specs {
				specs = append(specs, oracleSpecSig(sp))
			}
			parts = append(parts, "alter "+qual(st.Schema, st.Table)+"{"+strings.Join(specs, "; ")+"}")
		case "rename_table":
			pairs := make([]string, 0, len(st.Pairs))
			for _, p := range st.Pairs {
				pairs = append(pairs, qual(p.OldSchema, p.OldTable)+">"+qual(p.NewSchema, p.NewTable))
			}
			parts = append(parts, "rename "+strings.Join(pairs, ", "))
		}
	}
	return strings.Join(parts, " | "), false
}

func Diff(c run.Case, ourSig string, ourErr error, ourPanic *ddllexec.PanicInfo, d *digest.Digest) *Divergence {
	if ourPanic != nil {
		class := "panic"
		shape := NormalizePanic(ourPanic.Value, ourPanic.Stack)
		if ourPanic.Timeout {
			class = "timeout"
			shape = "head=" + HeadWord(c.SQL)
		}
		return &Divergence{Class: class, Shape: shape, OurSig: ourSig, OurError: errString(ourErr)}
	}
	if d == nil {
		return &Divergence{Class: "oracle_crash", Shape: "bad_digest", OurSig: ourSig, OurError: errString(ourErr)}
	}
	if d.Verdict == "reject" {
		return nil
	}
	if d.Verdict != "accept" {
		return &Divergence{Class: "oracle_crash", Shape: "bad_digest", OurSig: ourSig, OurError: errString(ourErr)}
	}
	oracleSig, skip := OracleSig(d)
	if skip {
		return nil
	}
	if ourErr != nil {
		return &Divergence{Class: "we_error", Shape: NormalizeError(ourErr.Error()), OurSig: ourSig, OracleSig: oracleSig, OurError: ourErr.Error()}
	}
	ourCanon, err := Canonicalize(ourSig, SideOur)
	if err != nil {
		return &Divergence{Class: "sig_mismatch", Shape: "parse_our_sig", OurSig: ourSig, OracleSig: oracleSig}
	}
	oracleCanon, err := Canonicalize(oracleSig, SideOracle)
	if err != nil {
		return &Divergence{Class: "sig_mismatch", Shape: "parse_oracle_sig", OurSig: ourSig, OracleSig: oracleSig}
	}
	if ourCanon == oracleCanon {
		return nil
	}
	return &Divergence{
		Class:     "sig_mismatch",
		Shape:     FirstDifference(ourCanon, oracleCanon),
		OurSig:    ourSig,
		OracleSig: oracleSig,
	}
}

func MakeDescriptor(c run.Case, lane string, div *Divergence) Descriptor {
	if lane == "" {
		lane = "fast"
	}
	return Descriptor{
		V:       1,
		Engine:  run.EngineName(c.Engine),
		SQLMode: MaskSQLMode(c.SQLMode),
		Class:   div.Class,
		Lane:    lane,
		Shape:   div.Shape,
	}
}

func DescriptorBytes(d Descriptor) []byte {
	keys := []string{"class", "engine", "lane", "shape", "sql_mode", "v"}
	vals := map[string]any{
		"v":        d.V,
		"engine":   d.Engine,
		"sql_mode": MaskSQLMode(d.SQLMode),
		"class":    d.Class,
		"lane":     d.Lane,
		"shape":    d.Shape,
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		vb, _ := json.Marshal(vals[k])
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes()
}

func DescriptorSig(d Descriptor) string {
	sum := sha256.Sum256(DescriptorBytes(d))
	return hex.EncodeToString(sum[:])[:12]
}

func MaskSQLMode(mode uint64) uint64 {
	return mode & descriptorMask
}

type Side int

const (
	SideOur Side = iota
	SideOracle
)

func Canonicalize(sig string, side Side) (string, error) {
	stmts, err := parseSignature(sig)
	if err != nil {
		return "", err
	}
	for si := range stmts {
		if stmts[si].kind != "alter" {
			continue
		}
		var flattened []sigSpec
		stmtPos := false
		for _, sp := range stmts[si].specs {
			if sp.pos {
				stmtPos = true
				sp.pos = false
			}
			if sp.kind == "col" && len(sp.cols) > 1 {
				for _, col := range sp.cols {
					cp := sp
					cp.cols = []sigCol{col}
					flattened = append(flattened, cp)
				}
				continue
			}
			if sp.kind == "chg" && len(sp.cols) == 1 && sp.old == sp.cols[0].name {
				sp.kind = "col"
				sp.old = ""
			}
			flattened = append(flattened, sp)
		}
		for i := range flattened {
			for j := range flattened[i].cols {
				if side == SideOur && flattened[i].cols[j].kind == string(types.QValueKindNumeric) {
					if flattened[i].cols[j].prec == -1 {
						flattened[i].cols[j].prec = 10
					}
					if flattened[i].cols[j].scale == -1 {
						flattened[i].cols[j].scale = 0
					}
				}
			}
		}
		sort.SliceStable(flattened, func(i, j int) bool {
			return specBucket(flattened[i].kind) < specBucket(flattened[j].kind)
		})
		stmts[si].specs = flattened
		stmts[si].pos = stmtPos
	}
	return renderSignature(stmts), nil
}

func FirstDifference(ours, theirs string) string {
	a, aerr := parseSignature(ours)
	b, berr := parseSignature(theirs)
	if aerr != nil || berr != nil {
		return "signature"
	}
	if len(a) != len(b) {
		return "stmt_count"
	}
	for i := range a {
		if a[i].kind != b[i].kind {
			return "stmt_kind"
		}
		if a[i].kind == "alter" {
			if a[i].qual != b[i].qual {
				return "table_qual"
			}
			if len(a[i].specs) != len(b[i].specs) {
				return "spec_count"
			}
			if a[i].pos != b[i].pos {
				return "position"
			}
			for j := range a[i].specs {
				as, bs := a[i].specs[j], b[i].specs[j]
				if as.kind != bs.kind {
					return "spec_kind"
				}
				if as.old != bs.old || as.new != bs.new {
					if as.kind == "ren" {
						return "rename_pairs"
					}
					return "col_name"
				}
				if len(as.cols) != len(bs.cols) {
					return "col_count"
				}
				for k := range as.cols {
					ac, bc := as.cols[k], bs.cols[k]
					if ac.name != bc.name {
						return "col_name"
					}
					if ac.kind != bc.kind {
						return fmt.Sprintf("col_kind(%s≠%s)", ac.kind, bc.kind)
					}
					if ac.prec != bc.prec || ac.scale != bc.scale {
						return fmt.Sprintf("col_params(%d,%d≠%d,%d)", ac.prec, ac.scale, bc.prec, bc.scale)
					}
					if ac.notNull != bc.notNull {
						return "col_notnull"
					}
				}
			}
		} else if !samePairs(a[i].pairs, b[i].pairs) {
			return "rename_pairs"
		}
	}
	return "signature"
}

// SplitTopLevel splits b on sep at paren depth 0, honoring string/quoted-ident
// delimiters, returning the segments (separators removed).
func SplitTopLevel(b []byte, sep byte) [][]byte {
	var out [][]byte
	start := 0
	depth := 0
	var quote byte
	lineComment := false
	blockComment := false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if lineComment {
			if c == '\n' || c == 0 {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if c == '*' && i+1 < len(b) && b[i+1] == '/' {
				i++
				blockComment = false
			}
			continue
		}
		if quote != 0 {
			switch quote {
			case '\'', '"':
				if c == '\\' && i+1 < len(b) {
					i++
					continue
				}
				if c == quote {
					if i+1 < len(b) && b[i+1] == quote {
						i++
						continue
					}
					quote = 0
				}
			case '`':
				if c == '`' {
					if i+1 < len(b) && b[i+1] == '`' {
						i++
						continue
					}
					quote = 0
				}
			case ']':
				if c == ']' {
					quote = 0
				}
			}
			continue
		}
		if c == '-' && i+2 < len(b) && b[i+1] == '-' && isSpace(b[i+2]) {
			lineComment = true
			i++
			continue
		}
		if c == '#' {
			lineComment = true
			continue
		}
		if c == '/' && i+1 < len(b) && b[i+1] == '*' {
			blockComment = true
			i++
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '[':
			quote = ']'
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if c == sep && depth == 0 {
				out = append(out, bytes.TrimSpace(b[start:i]))
				start = i + 1
			}
		}
	}
	out = append(out, bytes.TrimSpace(b[start:]))
	return out
}

func NormalizeError(s string) string {
	s = byteRE.ReplaceAllString(s, "at byte ?")
	s = quotedRE.ReplaceAllString(s, "\"?\"")
	s = wsRE.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func NormalizePanic(value string, stack []byte) string {
	first := value
	if idx := strings.IndexByte(first, '\n'); idx >= 0 {
		first = first[:idx]
	}
	first = hexAddrRE.ReplaceAllString(NormalizeError(first), "0x?")
	frame := firstConnmysqlFrame(string(stack))
	if frame != "" {
		return first + "@" + frame
	}
	return first
}

func HeadWord(sql []byte) string {
	s := strings.TrimLeftFunc(string(sql), unicode.IsSpace)
	if s == "" {
		return ""
	}
	for len(s) > 0 && strings.HasPrefix(s, "/*") {
		if end := strings.Index(s, "*/"); end >= 0 {
			s = strings.TrimLeftFunc(s[end+2:], unicode.IsSpace)
			continue
		}
		break
	}
	if strings.HasPrefix(s, "--") {
		if end := strings.IndexByte(s, '\n'); end >= 0 {
			s = strings.TrimLeftFunc(s[end+1:], unicode.IsSpace)
		}
	}
	end := 0
	for end < len(s) {
		r, size := utf8.DecodeRuneInString(s[end:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			break
		}
		end += size
	}
	return strings.ToUpper(s[:end])
}

var (
	byteRE    = regexp.MustCompile(`at byte \d+`)
	quotedRE  = regexp.MustCompile(`"(?s:[^"]*)"`)
	wsRE      = regexp.MustCompile(`\s+`)
	hexAddrRE = regexp.MustCompile(`0x[0-9a-fA-F]+`)
)

func oracleSpecSig(sp digest.Spec) string {
	var sb strings.Builder
	switch sp.Op {
	case "add", "modify":
		sb.WriteString("col ")
		for i, c := range sp.Cols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(oracleColSig(c))
		}
	case "change":
		sb.WriteString("chg ")
		sb.WriteString(sp.OldName)
		sb.WriteByte(' ')
		for i, c := range sp.Cols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(oracleColSig(c))
		}
	case "drop":
		sb.WriteString("drop ")
		sb.WriteString(sp.OldName)
	case "rename_col":
		sb.WriteString("ren ")
		sb.WriteString(sp.OldName)
		sb.WriteByte('>')
		sb.WriteString(sp.NewName)
	default:
		sb.WriteString("drop ")
		sb.WriteString(sp.OldName)
	}
	if sp.HasPosition {
		sb.WriteString(" @pos")
	}
	return sb.String()
}

func oracleColSig(c digest.Col) string {
	var sb strings.Builder
	sb.WriteString(c.Name)
	sb.WriteByte('=')
	kind, err := connmysql.QkindFromMysqlColumnType(c.TypeStr, true, 0)
	if err != nil {
		sb.WriteString("ERR")
	} else {
		sb.WriteString(string(kind))
	}
	if kind == types.QValueKindNumeric {
		p, s := -1, -1
		if len(c.ParamsWritten) > 0 {
			p = c.ParamsWritten[0]
		}
		if len(c.ParamsWritten) > 1 {
			s = c.ParamsWritten[1]
		}
		fmt.Fprintf(&sb, "(%d,%d)", p, s)
	}
	if c.NotNull {
		sb.WriteString(" nn")
	}
	return sb.String()
}

func qual(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f' || c == 0
}

func firstConnmysqlFrame(stack string) string {
	for _, line := range strings.Split(stack, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "connmysql.") {
			if idx := strings.Index(line, "connmysql."); idx >= 0 {
				name := line[idx+len("connmysql."):]
				if end := strings.IndexAny(name, "( "); end >= 0 {
					name = name[:end]
				}
				return name
			}
		}
	}
	return ""
}

type sigStmt struct {
	kind  string
	qual  string
	specs []sigSpec
	pos   bool
	pairs []sigPair
}

type sigSpec struct {
	kind string
	old  string
	new  string
	cols []sigCol
	pos  bool
}

type sigCol struct {
	name    string
	kind    string
	prec    int
	scale   int
	notNull bool
}

type sigPair struct {
	old string
	new string
}

func parseSignature(sig string) ([]sigStmt, error) {
	sig = strings.TrimSpace(sig)
	if sig == "" {
		return nil, nil
	}
	rawParts := splitStatements(sig)
	stmts := make([]sigStmt, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "alter "):
			st, err := parseAlter(part)
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, st)
		case strings.HasPrefix(part, "rename "):
			st, err := parseRename(part)
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, st)
		default:
			return nil, fmt.Errorf("unknown statement signature %q", part)
		}
	}
	return stmts, nil
}

func splitStatements(sig string) []string {
	var out []string
	depth := 0
	start := 0
	for i := 0; i < len(sig); i++ {
		switch sig[i] {
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case '|':
			if depth == 0 && i > 0 && i+1 < len(sig) && sig[i-1] == ' ' && sig[i+1] == ' ' {
				out = append(out, strings.TrimSpace(sig[start:i-1]))
				start = i + 2
			}
		}
	}
	out = append(out, strings.TrimSpace(sig[start:]))
	return out
}

func parseAlter(part string) (sigStmt, error) {
	body := strings.TrimPrefix(part, "alter ")
	open := strings.IndexByte(body, '{')
	close := strings.LastIndexByte(body, '}')
	if open < 0 || close < open {
		return sigStmt{}, errors.New("alter signature missing braces")
	}
	st := sigStmt{kind: "alter", qual: body[:open]}
	inside := body[open+1 : close]
	tail := strings.TrimSpace(body[close+1:])
	if tail == "@pos" {
		st.pos = true
	} else if tail != "" {
		return sigStmt{}, fmt.Errorf("unexpected alter tail %q", tail)
	}
	if strings.TrimSpace(inside) == "" {
		return st, nil
	}
	for _, raw := range SplitTopLevel([]byte(inside), ';') {
		sp, err := parseSpec(strings.TrimSpace(string(raw)))
		if err != nil {
			return sigStmt{}, err
		}
		st.specs = append(st.specs, sp)
	}
	return st, nil
}

func parseRename(part string) (sigStmt, error) {
	body := strings.TrimSpace(strings.TrimPrefix(part, "rename "))
	st := sigStmt{kind: "rename"}
	if body == "" {
		return st, nil
	}
	for _, raw := range SplitTopLevel([]byte(body), ',') {
		p := strings.TrimSpace(string(raw))
		old, newName, ok := strings.Cut(p, ">")
		if !ok {
			return sigStmt{}, fmt.Errorf("bad rename pair %q", p)
		}
		st.pairs = append(st.pairs, sigPair{old: old, new: newName})
	}
	return st, nil
}

func parseSpec(s string) (sigSpec, error) {
	var sp sigSpec
	if strings.HasSuffix(s, " @pos") {
		sp.pos = true
		s = strings.TrimSuffix(s, " @pos")
	}
	switch {
	case strings.HasPrefix(s, "col "):
		sp.kind = "col"
		cols, err := parseCols(strings.TrimPrefix(s, "col "))
		sp.cols = cols
		return sp, err
	case strings.HasPrefix(s, "chg "):
		sp.kind = "chg"
		rest := strings.TrimPrefix(s, "chg ")
		old, colsText, ok := strings.Cut(rest, " ")
		if !ok {
			return sp, fmt.Errorf("bad change spec %q", s)
		}
		sp.old = old
		cols, err := parseCols(colsText)
		sp.cols = cols
		return sp, err
	case strings.HasPrefix(s, "ren "):
		sp.kind = "ren"
		rest := strings.TrimPrefix(s, "ren ")
		old, newName, ok := strings.Cut(rest, ">")
		if !ok {
			return sp, fmt.Errorf("bad rename column spec %q", s)
		}
		sp.old, sp.new = old, newName
		return sp, nil
	case strings.HasPrefix(s, "drop "):
		sp.kind = "drop"
		sp.old = strings.TrimPrefix(s, "drop ")
		return sp, nil
	default:
		return sp, fmt.Errorf("bad spec %q", s)
	}
}

func parseCols(s string) ([]sigCol, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	parts := SplitTopLevel([]byte(s), ',')
	cols := make([]sigCol, 0, len(parts))
	for _, raw := range parts {
		col, err := parseCol(strings.TrimSpace(string(raw)))
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func parseCol(s string) (sigCol, error) {
	name, rest, ok := strings.Cut(s, "=")
	if !ok {
		return sigCol{}, fmt.Errorf("bad col %q", s)
	}
	col := sigCol{name: name, prec: -1, scale: -1}
	if strings.HasSuffix(rest, " nn") {
		col.notNull = true
		rest = strings.TrimSuffix(rest, " nn")
	}
	if idx := strings.IndexByte(rest, '('); idx >= 0 && strings.HasSuffix(rest, ")") {
		col.kind = rest[:idx]
		params := strings.TrimSuffix(rest[idx+1:], ")")
		p, q, ok := strings.Cut(params, ",")
		if !ok {
			return sigCol{}, fmt.Errorf("bad params %q", rest)
		}
		var err error
		col.prec, err = strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return sigCol{}, err
		}
		col.scale, err = strconv.Atoi(strings.TrimSpace(q))
		if err != nil {
			return sigCol{}, err
		}
	} else {
		col.kind = rest
	}
	return col, nil
}

func renderSignature(stmts []sigStmt) string {
	parts := make([]string, 0, len(stmts))
	for _, st := range stmts {
		switch st.kind {
		case "alter":
			specs := make([]string, 0, len(st.specs))
			for _, sp := range st.specs {
				specs = append(specs, renderSpec(sp))
			}
			part := "alter " + st.qual + "{" + strings.Join(specs, "; ") + "}"
			if st.pos {
				part += " @pos"
			}
			parts = append(parts, part)
		case "rename":
			pairs := make([]string, 0, len(st.pairs))
			for _, p := range st.pairs {
				pairs = append(pairs, p.old+">"+p.new)
			}
			parts = append(parts, "rename "+strings.Join(pairs, ", "))
		}
	}
	return strings.Join(parts, " | ")
}

func renderSpec(sp sigSpec) string {
	var out string
	switch sp.kind {
	case "col":
		out = "col " + renderCols(sp.cols)
	case "chg":
		out = "chg " + sp.old + " " + renderCols(sp.cols)
	case "ren":
		out = "ren " + sp.old + ">" + sp.new
	case "drop":
		out = "drop " + sp.old
	}
	if sp.pos {
		out += " @pos"
	}
	return out
}

func renderCols(cols []sigCol) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		s := c.name + "=" + c.kind
		if c.kind == string(types.QValueKindNumeric) {
			s += fmt.Sprintf("(%d,%d)", c.prec, c.scale)
		}
		if c.notNull {
			s += " nn"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

func specBucket(kind string) int {
	switch kind {
	case "col", "chg":
		return 0
	case "ren":
		return 1
	case "drop":
		return 2
	default:
		return 3
	}
}

func samePairs(a, b []sigPair) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
