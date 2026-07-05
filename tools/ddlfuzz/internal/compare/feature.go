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
// Feature 0 is the structural class: engine, masked sql_mode, oracle verdict,
// masked error class, and a flat accept shape per side — statement-kind list
// (capped), statement-count bucket, qualification/position/rename-to flags,
// pair-count bucket, and spec-kind counts bucketed {0,1,2,3+}. It drops
// everything near-injective over inputs: identifiers, type params, digits,
// spec order, exact counts. Column type families are deliberately *not* part
// of the structural tuple — family sets are combinatorial (measured on the
// live corpus: 13.5K flat classes vs 66K with family sets) — instead each
// distinct family in the case emits its own (engine, mode, family) feature,
// so a never-seen type family still retains without multiplying the
// structural space.
//
// The oracle side is read straight off the structured digest; the parser side
// scans the signature string with the same backtick-aware splitters the
// signature parser uses, without building the sigStmt tree. FNV-1a with the
// standard basis, not maphash: features index a bitmap that persists across
// restarts, so they must be stable across processes.
func BehaviorFeatures(c run.Case, ourSig string, ourErr error, ourPanic *ddllexec.PanicInfo, d *digest.Digest, buf []uint64) []uint64 {
	var fams famSet
	f := featureHasher{sum: fnvOffset64}
	f.byte(c.Engine)
	f.u64(MaskSQLMode(c.SQLMode))
	switch {
	case d == nil:
		f.byte(0)
	case d.Verdict == "accept":
		f.byte(1)
		f.digestShape(d, &fams)
	case d.Verdict == "reject":
		f.byte(2)
		f.oracleErrorClass(d.Error)
	default:
		f.byte(3)
		f.str(d.Verdict)
		f.oracleErrorClass(d.Error)
	}
	if ourSig != "" {
		f.byte(1)
		f.sigShape(ourSig, &fams)
	} else {
		f.byte(0)
	}
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

	base := featureHasher{sum: fnvOffset64}
	base.byte(c.Engine)
	base.u64(MaskSQLMode(c.SQLMode))
	base.byte('F')
	for i := range fams.n {
		g := base
		g.u64(fams.h[i])
		buf = append(buf, g.sum)
	}
	if fams.overflow {
		g := base
		g.str("overflow")
		buf = append(buf, g.sum)
	}
	return buf
}

// BehaviorFeature is the structural class alone (BehaviorFeatures feature 0)
// for callers that need one grouping key per case, e.g. corpus-distill.
func BehaviorFeature(c run.Case, ourSig string, ourErr error, ourPanic *ddllexec.PanicInfo, d *digest.Digest) uint64 {
	var buf [1 + maxFamilies + 1]uint64
	return BehaviorFeatures(c, ourSig, ourErr, ourPanic, d, buf[:0])[0]
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211

	// Statements past the cap still count toward the length bucket but add no
	// kind letters, so statement-list combinatorics stay bounded.
	maxCoarseStmts = 4

	// famSet overflow bound; a case touching more type families than this
	// gets one shared overflow feature for the rest.
	maxFamilies = 12
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
				end = strings.IndexByte(s[i+1:], c)
			}
			if end >= 0 {
				f.byte(c)
				f.byte('?')
				f.byte(c)
				prev = c
				i += end + 2
				continue
			}
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

func isWordByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// flatShape is one side's accept shape, flattened across statements. Spec
// counts index: col (add/modify), chg, ren, drop, unknown.
type flatShape struct {
	kinds    [maxCoarseStmts]byte
	n        int
	qual     bool
	pos      bool
	renameTo bool
	pairs    int
	counts   [5]int
}

func (fs *flatShape) addKind(k byte) {
	if fs.n < maxCoarseStmts {
		fs.kinds[fs.n] = k
	}
	fs.n++
}

func (f *featureHasher) flat(fs *flatShape) {
	for i := 0; i < min(fs.n, maxCoarseStmts); i++ {
		f.byte(fs.kinds[i])
	}
	f.byte(bucket4(fs.n))
	f.bool(fs.qual)
	f.bool(fs.pos)
	f.bool(fs.renameTo)
	f.byte(bucket4(fs.pairs))
	for _, c := range fs.counts {
		f.byte(bucket4(c))
	}
}

func (f *featureHasher) digestShape(d *digest.Digest, fams *famSet) {
	stmts := d.Stmts
	if len(stmts) > 1 && stmts[0].Kind == "other" {
		// Mirror OracleSig's skip: multi-statement input starting with a
		// non-DDL statement is never signature-compared.
		f.byte('s')
		return
	}
	var fs flatShape
	for _, st := range stmts {
		if st.Kind == "other" {
			break
		}
		switch st.Kind {
		case "alter_table":
			fs.addKind('a')
			fs.qual = fs.qual || st.Schema != ""
			fs.renameTo = fs.renameTo || (st.NewTable != "" && (st.NewSchema != st.Schema || st.NewTable != st.Table))
			for _, sp := range st.Specs {
				fs.pos = fs.pos || sp.HasPosition
				switch sp.Op {
				case "add", "modify":
					fs.counts[0]++
				case "change":
					fs.counts[1]++
				case "rename_col":
					fs.counts[2]++
				case "drop":
					fs.counts[3]++
				default:
					fs.counts[4]++
				}
				for _, col := range sp.Cols {
					fams.add(famHash(col.TypeStr))
				}
			}
		case "rename_table":
			fs.addKind('r')
			fs.pairs += len(st.Pairs)
			for _, p := range st.Pairs {
				fs.qual = fs.qual || p.OldSchema != "" || p.NewSchema != ""
			}
		}
	}
	f.flat(&fs)
}

// sigShape coarsens a signature string. Malformed pieces fall into a '?'
// statement kind instead of an error — a sig our own renderer wouldn't
// produce is itself a behavior worth one bit.
func (f *featureHasher) sigShape(sig string, fams *famSet) {
	var fs flatShape
	for _, part := range splitStatements(strings.TrimSpace(sig)) {
		if part == "" {
			continue
		}
		switch {
		case strings.HasPrefix(part, "alter "):
			alterSigShape(&fs, fams, part[len("alter "):])
		case strings.HasPrefix(part, "rename "):
			fs.addKind('r')
			pairs := splitSignatureTopLevel(strings.TrimSpace(part[len("rename "):]), ',')
			fs.pairs += len(pairs)
			for _, p := range pairs {
				if _, _, ok := cutSignatureSep(p, '.'); ok {
					fs.qual = true
				}
			}
		default:
			fs.addKind('?')
		}
	}
	f.flat(&fs)
}

func alterSigShape(fs *flatShape, fams *famSet, body string) {
	open, close := alterBraceIndexes(body)
	if open < 0 || close < open {
		fs.addKind('?')
		return
	}
	fs.addKind('a')
	if _, _, ok := cutSignatureSep(body[:open], '.'); ok {
		fs.qual = true
	}
	if strings.TrimSpace(body[close+1:]) == "@pos" {
		fs.pos = true
	}
	inside := body[open+1 : close]
	if strings.TrimSpace(inside) == "" {
		return
	}
	for _, raw := range splitSignatureTopLevel(inside, ';') {
		sp := strings.TrimSpace(raw)
		if strings.HasSuffix(sp, " @pos") {
			fs.pos = true
			sp = strings.TrimSuffix(sp, " @pos")
		}
		switch {
		case strings.HasPrefix(sp, "col "):
			fs.counts[0]++
			colFamilies(fams, sp[len("col "):])
		case strings.HasPrefix(sp, "chg "):
			fs.counts[1]++
			colFamilies(fams, sp[len("chg "):])
		case strings.HasPrefix(sp, "ren "):
			fs.counts[2]++
		case strings.HasPrefix(sp, "drop "):
			fs.counts[3]++
		default:
			fs.counts[4]++
		}
	}
}

// colFamilies extracts the type family of each `name=kind[(p,s)][ nn]` column
// in a col/chg spec body. For chg the leading old-name token has no top-level
// '=', so the first cut still lands on the column's kind.
func colFamilies(fams *famSet, cols string) {
	for _, raw := range splitSignatureTopLevel(cols, ',') {
		_, rest, ok := cutSignatureSep(raw, '=')
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		if idx := strings.IndexAny(rest, "( "); idx >= 0 {
			rest = rest[:idx]
		}
		fams.add(famHash(rest))
	}
}

func bucket4(n int) byte {
	if n > 3 {
		return 3
	}
	return byte(n)
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
