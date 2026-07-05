package mutate

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"unicode/utf8"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/dict"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

const sentinelDB = "peerdb_ddlfuzz_nodb"

func Mutate(r *rand.Rand, c run.Case) run.Case {
	return MutateWithSplice(r, c, nil)
}

func MutateWithSplice(r *rand.Rand, c run.Case, mate *run.Case) run.Case {
	if r == nil {
		r = rand.New(rand.NewPCG(3, 4))
	}
	out := c
	// No defensive copy: every arm below is copy-on-write, so c.SQL is never
	// mutated in place; out.SQL may alias c.SQL when no arm rewrites it.
	sql := c.SQL
	arms := 8
	if mate == nil {
		arms = 7
	}
	for i, n := 0, 1+r.IntN(3); i < n; i++ {
		prev := sql
		op := r.IntN(arms)
		if mate == nil && op >= 5 {
			op++ // splice arm needs a mate; keep the remaining arms uniform
		}
		switch op {
		case 0:
			sql = deleteToken(r, sql)
		case 1:
			sql = duplicateToken(r, sql)
		case 2:
			sql = dictionarySub(r, sql, run.EngineName(c.Engine))
		case 3:
			sql = dictionaryInsert(r, sql, run.EngineName(c.Engine))
		case 4:
			sql = quoteFlip(r, sql)
		case 5:
			sql = Crossover(r, sql, mate.SQL)
		case 6:
			out.SQLMode = flipSQLMode(r, out.SQLMode)
		default:
			sql = byteOp(r, sql)
		}
		sql = validOrPrevious(prev, sql)
	}
	out.SQL = sql
	out.Origin = run.OriginMut
	return out
}

func Tokens(sql []byte) [][]byte {
	// Presized for typical SQL token density (~1 token per 4 bytes) so the
	// append loop almost never regrows; delimiter-heavy inputs regrow at most
	// twice instead of ~log2(n) times from nil.
	toks := make([][]byte, 0, len(sql)/4+8)
	start := -1
	var quote byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(sql) && sql[i+1] == quote {
					i++
					continue
				}
				quote = 0
			} else if c == '\\' && i+1 < len(sql) {
				i++
			}
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			if start < 0 {
				start = i
			}
			quote = c
			continue
		}
		if isDelim(c) {
			if start >= 0 {
				toks = append(toks, sql[start:i])
				start = -1
			}
			if !isSpace(c) {
				toks = append(toks, sql[i:i+1])
			}
			continue
		}
		if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		toks = append(toks, sql[start:])
	}
	return toks
}

func deleteToken(r *rand.Rand, sql []byte) []byte {
	toks := Tokens(sql)
	if len(toks) == 0 {
		return sql
	}
	tok := toks[r.IntN(len(toks))]
	return bytes.Replace(sql, tok, nil, 1)
}

func duplicateToken(r *rand.Rand, sql []byte) []byte {
	toks := Tokens(sql)
	if len(toks) == 0 {
		return sql
	}
	tok := toks[r.IntN(len(toks))]
	dup := make([]byte, 0, 2*len(tok)+1)
	dup = append(dup, tok...)
	dup = append(dup, ' ')
	dup = append(dup, tok...)
	return bytes.Replace(sql, tok, dup, 1)
}

func dictionarySub(r *rand.Rand, sql []byte, engine string) []byte {
	toks := Tokens(sql)
	d := dict.Tokens(engine)
	if len(toks) == 0 || len(d) == 0 {
		return sql
	}
	tok := toks[r.IntN(len(toks))]
	repl := []byte(d[r.IntN(len(d))])
	return bytes.Replace(sql, tok, repl, 1)
}

func dictionaryInsert(r *rand.Rand, sql []byte, engine string) []byte {
	d := dict.Tokens(engine)
	if len(d) == 0 {
		return sql
	}
	pos := tokenBoundary(r, sql)
	insert := d[r.IntN(len(d))]
	out := make([]byte, 0, len(sql)+len(insert)+2)
	out = append(out, sql[:pos]...)
	if pos > 0 && len(out) > 0 && !isSpace(out[len(out)-1]) {
		out = append(out, ' ')
	}
	out = append(out, insert...)
	if pos < len(sql) && len(insert) > 0 && !isSpace(sql[pos]) {
		out = append(out, ' ')
	}
	out = append(out, sql[pos:]...)
	return out
}

func quoteFlip(r *rand.Rand, sql []byte) []byte {
	wrappers := [][2]string{{"/*!", " */"}, {"/*M!", " */"}, {"`", "`"}, {"\"", "\""}, {"-- ", "\n"}, {"#", "\n"}}
	w := wrappers[r.IntN(len(wrappers))]
	out := make([]byte, 0, len(w[0])+len(sql)+len(w[1]))
	out = append(out, w[0]...)
	out = append(out, sql...)
	return append(out, w[1]...)
}

func byteOp(r *rand.Rand, sql []byte) []byte {
	printables := " abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_,()'`"
	if len(sql) == 0 || r.IntN(3) == 0 {
		pos := 0
		if len(sql) > 0 {
			pos = r.IntN(len(sql) + 1)
		}
		out := make([]byte, 0, len(sql)+1)
		out = append(out, sql[:pos]...)
		out = append(out, printables[r.IntN(len(printables))])
		return append(out, sql[pos:]...)
	}
	pos := r.IntN(len(sql))
	if r.IntN(2) == 0 {
		out := make([]byte, 0, len(sql)-1)
		out = append(out, sql[:pos]...)
		return append(out, sql[pos+1:]...)
	}
	out := make([]byte, len(sql))
	copy(out, sql)
	out[pos] = printables[r.IntN(len(printables))]
	return out
}

func Crossover(r *rand.Rand, a, b []byte) []byte {
	if r == nil {
		r = rand.New(rand.NewPCG(7, 8))
	}
	ai := tokenBoundary(r, a)
	bi := tokenBoundary(r, b)
	if ai <= 0 || bi >= len(b) {
		return append([]byte{}, a...)
	}
	out := make([]byte, 0, ai+len(b)-bi)
	out = append(out, a[:ai]...)
	return append(out, b[bi:]...)
}

func flipSQLMode(r *rand.Rand, mode uint64) uint64 {
	bits := []uint64{
		compare.SQLModeANSIQuotes,
		compare.SQLModeOracle,
		compare.SQLModeMSSQL,
		compare.SQLModeNoBackslashEscapes,
	}
	return mode ^ bits[r.IntN(len(bits))]
}

func tokenBoundary(r *rand.Rand, sql []byte) int {
	toks := Tokens(sql)
	if len(toks) == 0 {
		if len(sql) == 0 {
			return 0
		}
		return r.IntN(len(sql) + 1)
	}
	tok := toks[r.IntN(len(toks))]
	idx := bytes.Index(sql, tok)
	if idx < 0 {
		return r.IntN(len(sql) + 1)
	}
	if r.IntN(2) == 0 {
		return idx
	}
	return idx + len(tok)
}

func validOrPrevious(prev, sql []byte) []byte {
	if !utf8.Valid(sql) || bytes.Contains(sql, []byte(sentinelDB)) {
		return prev
	}
	return sql
}

func isDelim(c byte) bool {
	return isSpace(c) || strings.ContainsRune(",();", rune(c))
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}
