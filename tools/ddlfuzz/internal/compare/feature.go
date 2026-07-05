package compare

import (
	"strings"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

// BehaviorFeatures reduces one execution to coarse behavior features,
// appending them to buf; callers mask each down to a virgin bitmap index and
// retain iff any bit is new.
//
// Feature 0 is the chain residue: engine, masked sql_mode, oracle verdict,
// masked error class, parser error/panic class, and each side's statement-count
// bucket. Per-statement shapes, kind bigrams, and type families each emit
// independent features from bounded spaces, so no feature hashes a
// cross-statement product.
//
// The oracle side is read straight off the structured digest; the parser side
// scans the signature string with the same backtick-aware splitters the
// signature parser uses, without building the sigStmt tree. FNV-1a with the
// standard basis, not maphash: features index a bitmap that persists across
// restarts, so they must be stable across processes.
func BehaviorFeatures(c run.Case, ourSig string, ourErr error, ourPanic *ddllexec.PanicInfo, d *digest.Digest, buf []uint64) []uint64 {
	feats, _ := BehaviorFeaturesKinded(c, ourSig, ourErr, ourPanic, d, buf, nil)
	return feats
}

type FeatureKind byte

const (
	FeatureKindChain  FeatureKind = 'C'
	FeatureKindStmt   FeatureKind = 'S'
	FeatureKindBigram FeatureKind = 'B'
	FeatureKindFamily FeatureKind = 'F'
)

// BehaviorFeaturesKinded is BehaviorFeatures plus an optional parallel kind
// slice for stats callers. Passing nil keeps the hot path allocation-free.
func BehaviorFeaturesKinded(c run.Case, ourSig string, ourErr error, ourPanic *ddllexec.PanicInfo, d *digest.Digest, buf []uint64, kinds []FeatureKind) ([]uint64, []FeatureKind) {
	var fams famSet
	var shapes shapeSet
	var bigrams bigramSet
	var digestStmts, sigStmts int
	f := featureHasher{sum: fnvOffset64}
	f.byte(c.Engine)
	f.u64(MaskSQLMode(c.SQLMode))
	switch {
	case d == nil:
		f.byte(0)
	case d.Verdict == "accept":
		f.byte(1)
		digestStmts = digestShape(d, &shapes, &bigrams, &fams)
	case d.Verdict == "reject":
		f.byte(2)
		f.oracleErrorClass(d.Error)
	default:
		f.byte(3)
		f.str(d.Verdict)
		f.oracleErrorClass(d.Error)
	}
	f.byte(bucket4(digestStmts))
	if ourSig != "" {
		f.byte(1)
		sigStmts = sigShape(ourSig, &shapes, &bigrams, &fams)
	} else {
		f.byte(0)
	}
	f.byte(bucket4(sigStmts))
	if ourErr != nil {
		f.byte(1)
		f.maskedError(ourErr.Error())
	} else {
		f.byte(0)
	}
	switch {
	case ourPanic == nil:
		f.byte(0)
	case ourPanic.Timeout:
		f.byte(2)
	default:
		// Panics are rare enough that NormalizePanic's regexes don't matter.
		f.byte(1)
		f.str(NormalizePanic(ourPanic.Value, ourPanic.Stack))
	}
	buf = append(buf, f.sum)
	if kinds != nil {
		kinds = append(kinds, FeatureKindChain)
	}

	base := featureHasher{sum: fnvOffset64}
	base.byte(c.Engine)
	base.u64(MaskSQLMode(c.SQLMode))
	base.byte('S')
	for i := range shapes.n {
		g := base
		g.u64(shapes.h[i])
		buf = append(buf, g.sum)
		if kinds != nil {
			kinds = append(kinds, FeatureKindStmt)
		}
	}
	if shapes.overflow {
		g := base
		g.str("overflow")
		buf = append(buf, g.sum)
		if kinds != nil {
			kinds = append(kinds, FeatureKindStmt)
		}
	}

	base = featureHasher{sum: fnvOffset64}
	base.byte(c.Engine)
	base.u64(MaskSQLMode(c.SQLMode))
	base.byte('B')
	for i := range bigrams.n {
		g := base
		g.byte(bigrams.a[i])
		g.byte(bigrams.b[i])
		buf = append(buf, g.sum)
		if kinds != nil {
			kinds = append(kinds, FeatureKindBigram)
		}
	}

	base = featureHasher{sum: fnvOffset64}
	base.byte(c.Engine)
	base.u64(MaskSQLMode(c.SQLMode))
	base.byte('F')
	for i := range fams.n {
		g := base
		g.u64(fams.h[i])
		buf = append(buf, g.sum)
		if kinds != nil {
			kinds = append(kinds, FeatureKindFamily)
		}
	}
	if fams.overflow {
		g := base
		g.str("overflow")
		buf = append(buf, g.sum)
		if kinds != nil {
			kinds = append(kinds, FeatureKindFamily)
		}
	}
	return buf, kinds
}

// CaseKey folds an emitted behavior feature sequence into one stable grouping
// key for corpus stamps and distill.
func CaseKey(feats []uint64) uint64 {
	f := featureHasher{sum: fnvOffset64}
	for _, feat := range feats {
		f.u64(feat)
	}
	return f.sum
}

// BehaviorFeature is the full behavior feature fold for callers that need one
// grouping key per case, e.g. corpus-distill.
func BehaviorFeature(c run.Case, ourSig string, ourErr error, ourPanic *ddllexec.PanicInfo, d *digest.Digest) uint64 {
	var buf [1 + maxShapes + 1 + maxBigrams + maxFamilies + 1]uint64
	return CaseKey(BehaviorFeatures(c, ourSig, ourErr, ourPanic, d, buf[:0]))
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211

	// famSet overflow bound; a case touching more type families than this
	// gets one shared overflow feature for the rest.
	maxFamilies = 12

	// shapeSet overflow bound; statement-shape novelty above this per case
	// collapses to one shared overflow feature.
	maxShapes = 16

	maxBigrams = 16
)

type featureHasher struct{ sum uint64 }

func (f *featureHasher) byte(b byte) {
	f.sum = (f.sum ^ uint64(b)) * fnvPrime64
}

func (f *featureHasher) bool(v bool) {
	if v {
		f.byte(1)
	} else {
		f.byte(0)
	}
}

func (f *featureHasher) u64(v uint64) {
	for range 8 {
		f.byte(byte(v))
		v >>= 8
	}
}

func (f *featureHasher) str(s string) {
	for i := 0; i < len(s); i++ {
		f.byte(s[i])
	}
	f.byte(0xFF)
}

// oracleErrorClass hashes the errno prefix of a driver-formatted oracle error
// ("1064: ...") and nothing else. Several server error families echo raw
// input into the message unquoted (Undeclared variable, Undefined CONDITION,
// Incorrect routine name, ...), so any text mask stays input-unique — the
// errno is the only bounded field. No errno prefix falls back to the masked
// text.
func (f *featureHasher) oracleErrorClass(s string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i > 0 && i < len(s) && s[i] == ':' {
		f.byte(1)
		f.str(s[:i])
		return
	}
	f.byte(0)
	f.maskedError(s)
}

// maskedError is the streaming twin of NormalizeError: quoted spans, digit
// runs, and whitespace runs collapse to placeholders on the way into the
// hash, with no regexes or intermediate strings on the per-exec path. Same
// apostrophe-in-word guard, same over-mask-never-under-mask rule.
func (f *featureHasher) maskedError(s string) {
	var prev byte
	i := 0
	for i < len(s) {
		c := s[i]
		if c == '"' || c == '`' || (c == '\'' && !isWordByte(prev)) {
			end := -1
			// The servers' near-fragment quotes verbatim input, which may
			// itself contain quotes (`near ''x' INT' at line 1`); mask
			// through the last closer so fragment content never leaks.
			if c == '\'' && i >= 5 && s[i-5:i] == "near " {
				end = strings.LastIndex(s[i+1:], "' at line ")
			}
			if end < 0 {
				// Skip backslash escapes: Go %q-formatted fragments escape
				// embedded quotes, and a bare IndexByte would take the first
				// escaped quote as a false closer, leaking the raw tail.
				end = indexUnescaped(s[i+1:], c)
			}
			if end < 0 {
				// Unterminated span: mask through end of string.
				f.byte(c)
				f.byte('?')
				f.byte(c)
				break
			}
			f.byte(c)
			f.byte('?')
			f.byte(c)
			prev = c
			i += end + 2
			continue
		}
		switch {
		case c >= '0' && c <= '9':
			f.byte('#')
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
			prev = '#'
		case isSpaceByte(c):
			f.byte(' ')
			for i < len(s) && isSpaceByte(s[i]) {
				i++
			}
			prev = ' '
		default:
			f.byte(c)
			prev = c
			i++
		}
	}
	f.byte(0xFF)
}

// indexUnescaped returns the index of the first c in s not preceded by a
// backslash, or -1.
func indexUnescaped(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case c:
			return i
		}
	}
	return -1
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// stmtShape is one statement's accept shape. Spec counts index: col
// (add/modify), chg, ren, drop, unknown.
type stmtShape struct {
	kind   byte
	qual   bool
	pos    bool
	pairs  int
	counts [5]int
}

func (s stmtShape) hash() uint64 {
	f := featureHasher{sum: fnvOffset64}
	f.byte(s.kind)
	f.bool(s.qual)
	f.bool(s.pos)
	f.byte(bucket4(s.pairs))
	for _, c := range s.counts {
		f.byte(bucket4(c))
	}
	return f.sum
}

func digestShape(d *digest.Digest, shapes *shapeSet, bigrams *bigramSet, fams *famSet) int {
	stmts := d.Stmts
	if len(stmts) > 1 && stmts[0].Kind == "other" {
		// Mirror OracleSig's skip: multi-statement input starting with a
		// non-DDL statement is never signature-compared.
		shapes.add(stmtShape{kind: 's'}.hash())
		return 1
	}
	var prev byte
	n := 0
	emit := func(sh stmtShape) {
		if prev != 0 {
			bigrams.add(prev, sh.kind)
		}
		prev = sh.kind
		shapes.add(sh.hash())
		n++
	}
	for _, st := range stmts {
		if st.Kind == "other" {
			break
		}
		switch st.Kind {
		case "alter_table":
			// Mirror OracleSig's rename normalization: a non-noop rename
			// splits off as a trailing standalone rename statement, and an
			// alter left with no other specs is dropped — agreeing sides
			// collapse to the same shapes.
			renamed := st.NewTable != "" && (st.NewSchema != st.Schema || st.NewTable != st.Table)
			if !renamed || len(st.Specs) > 0 {
				sh := stmtShape{kind: 'a', qual: st.Schema != ""}
				for _, sp := range st.Specs {
					sh.pos = sh.pos || sp.HasPosition
					switch sp.Op {
					case "add", "modify":
						sh.counts[0]++
					case "change":
						sh.counts[1]++
					case "rename_col":
						sh.counts[2]++
					case "drop":
						sh.counts[3]++
					default:
						sh.counts[4]++
					}
					for _, col := range sp.Cols {
						fams.add(famHash(col.TypeStr))
					}
				}
				emit(sh)
			}
			if renamed {
				emit(stmtShape{kind: 'r', pairs: 1, qual: st.Schema != "" || st.NewSchema != ""})
			}
		case "rename_table":
			sh := stmtShape{kind: 'r', pairs: len(st.Pairs)}
			for _, p := range st.Pairs {
				sh.qual = sh.qual || p.OldSchema != "" || p.NewSchema != ""
			}
			emit(sh)
		default:
			emit(stmtShape{kind: '?'})
		}
	}
	return n
}

// sigShape coarsens a signature string. Malformed pieces fall into a '?'
// statement kind instead of an error — a sig our own renderer wouldn't
// produce is itself a behavior worth one bit.
func sigShape(sig string, shapes *shapeSet, bigrams *bigramSet, fams *famSet) int {
	var prev byte
	n := 0
	forEachFeatureStmt(strings.TrimSpace(sig), func(part string) {
		if part == "" {
			return
		}
		var sh stmtShape
		switch {
		case strings.HasPrefix(part, "alter "):
			sh = alterSigShape(fams, part[len("alter "):])
		case strings.HasPrefix(part, "rename "):
			sh.kind = 'r'
			forEachFeatureTopLevel(strings.TrimSpace(part[len("rename "):]), ',', func(p string) {
				sh.pairs++
				if _, _, ok := cutSignatureSep(p, '.'); ok {
					sh.qual = true
				}
			})
		default:
			sh.kind = '?'
		}
		if prev != 0 {
			bigrams.add(prev, sh.kind)
		}
		prev = sh.kind
		shapes.add(sh.hash())
		n++
	})
	return n
}

func alterSigShape(fams *famSet, body string) stmtShape {
	sh := stmtShape{kind: 'a'}
	open, close := alterBraceIndexes(body)
	if open < 0 || close < open {
		return stmtShape{kind: '?'}
	}
	if _, _, ok := cutSignatureSep(body[:open], '.'); ok {
		sh.qual = true
	}
	if strings.TrimSpace(body[close+1:]) == "@pos" {
		sh.pos = true
	}
	inside := body[open+1 : close]
	if strings.TrimSpace(inside) == "" {
		return sh
	}
	forEachFeatureTopLevel(inside, ';', func(raw string) {
		sp := strings.TrimSpace(raw)
		if strings.HasSuffix(sp, " @pos") {
			sh.pos = true
			sp = strings.TrimSuffix(sp, " @pos")
		}
		switch {
		case strings.HasPrefix(sp, "col "):
			sh.counts[0]++
			colFamilies(fams, sp[len("col "):])
		case strings.HasPrefix(sp, "chg "):
			sh.counts[1]++
			colFamilies(fams, sp[len("chg "):])
		case strings.HasPrefix(sp, "ren "):
			sh.counts[2]++
		case strings.HasPrefix(sp, "drop "):
			sh.counts[3]++
		default:
			sh.counts[4]++
		}
	})
	return sh
}

// colFamilies extracts the type family of each `name=kind[(p,s)][ nn]` column
// in a col/chg spec body. For chg the leading old-name token has no top-level
// '=', so the first cut still lands on the column's kind.
func colFamilies(fams *famSet, cols string) {
	forEachFeatureTopLevel(cols, ',', func(raw string) {
		_, rest, ok := cutSignatureSep(raw, '=')
		if !ok {
			return
		}
		rest = strings.TrimSpace(rest)
		if idx := strings.IndexAny(rest, "( "); idx >= 0 {
			rest = rest[:idx]
		}
		fams.add(famHash(rest))
	})
}

func forEachFeatureStmt(sig string, fn func(string)) {
	depth := 0
	start := 0
	inIdent := false
	for i := 0; i < len(sig); i++ {
		if inIdent {
			if sig[i] == '`' {
				if i+1 < len(sig) && sig[i+1] == '`' {
					i++
					continue
				}
				inIdent = false
			}
			continue
		}
		switch sig[i] {
		case '`':
			if !sigQuoteStarts(sig, i) {
				continue
			}
			inIdent = true
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case '|':
			if depth == 0 && i > 0 && i+1 < len(sig) && sig[i-1] == ' ' && sig[i+1] == ' ' {
				fn(strings.TrimSpace(sig[start : i-1]))
				start = i + 2
			}
		}
	}
	fn(strings.TrimSpace(sig[start:]))
}

func forEachFeatureTopLevel(s string, sep byte, fn func(string)) {
	start := 0
	depth := 0
	inIdent := false
	for i := 0; i < len(s); i++ {
		if inIdent {
			if s[i] == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inIdent = false
			}
			continue
		}
		switch s[i] {
		case '`':
			if !sigQuoteStarts(s, i) {
				continue
			}
			inIdent = true
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if s[i] == sep && depth == 0 {
				fn(strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	fn(strings.TrimSpace(s[start:]))
}

func bucket4(n int) byte {
	if n > 3 {
		return 3
	}
	return byte(n)
}

// shapeSet is a small dedup set of per-statement shape hashes in first-seen
// order; each member becomes its own feature.
type shapeSet struct {
	h        [maxShapes]uint64
	n        int
	overflow bool
}

func (s *shapeSet) add(h uint64) {
	for i := range s.n {
		if s.h[i] == h {
			return
		}
	}
	if s.n == maxShapes {
		s.overflow = true
		return
	}
	s.h[s.n] = h
	s.n++
}

type bigramSet struct {
	a [maxBigrams]byte
	b [maxBigrams]byte
	n int
}

func (s *bigramSet) add(a, b byte) {
	for i := range s.n {
		if s.a[i] == a && s.b[i] == b {
			return
		}
	}
	if s.n == maxBigrams {
		return
	}
	s.a[s.n], s.b[s.n] = a, b
	s.n++
}

// famSet is a small dedup set of type-family hashes in first-seen order;
// each member becomes its own feature, so ordering never reaches the bitmap.
type famSet struct {
	h        [maxFamilies]uint64
	n        int
	overflow bool
}

func (s *famSet) add(h uint64) {
	for i := range s.n {
		if s.h[i] == h {
			return
		}
	}
	if s.n == maxFamilies {
		s.overflow = true
		return
	}
	s.h[s.n] = h
	s.n++
}

// famHash reduces a written type to its family: parenthesized params and all
// digits drop out, so numeric(10,2)→numeric, tinyint(1) unsigned→tinyint
// unsigned, and int32/int64 collapse to int (over-collapse by design — the
// finding pipeline still sees exact kinds, retention feedback doesn't need
// to).
func famHash(s string) uint64 {
	h := uint64(fnvOffset64)
	depth := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '(':
			depth++
		case c == ')':
			if depth > 0 {
				depth--
			}
		case depth == 0 && (c < '0' || c > '9'):
			h = (h ^ uint64(c)) * fnvPrime64
		}
	}
	return h
}
