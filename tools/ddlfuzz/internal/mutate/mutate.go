package mutate

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"unicode/utf8"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/dict"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func Mutate(r *rand.Rand, c run.Case) run.Case {
	if r == nil {
		r = rand.New(rand.NewPCG(3, 4))
	}
	out := c
	sql := append([]byte(nil), c.SQL...)
	for i, n := 0, 1+r.IntN(3); i < n; i++ {
		switch r.IntN(5) {
		case 0:
			sql = deleteToken(r, sql)
		case 1:
			sql = duplicateToken(r, sql)
		case 2:
			sql = dictionarySub(r, sql, run.EngineName(c.Engine))
		case 3:
			sql = quoteFlip(r, sql)
		default:
			sql = byteOp(r, sql)
		}
		if !utf8.Valid(sql) {
			sql = bytes.ToValidUTF8(sql, nil)
		}
	}
	out.SQL = sql
	out.Origin = run.OriginMut
	return out
}

func Tokens(sql []byte) [][]byte {
	var toks [][]byte
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
	return bytes.Replace(sql, tok, append(append([]byte{}, tok...), append([]byte(" "), tok...)...), 1)
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

func quoteFlip(r *rand.Rand, sql []byte) []byte {
	wrappers := [][2]string{{"/*!", " */"}, {"/*M!", " */"}, {"`", "`"}, {"\"", "\""}, {"-- ", "\n"}, {"#", "\n"}}
	w := wrappers[r.IntN(len(wrappers))]
	return []byte(w[0] + string(sql) + w[1])
}

func byteOp(r *rand.Rand, sql []byte) []byte {
	printables := " abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_,()'`"
	if len(sql) == 0 || r.IntN(3) == 0 {
		pos := 0
		if len(sql) > 0 {
			pos = r.IntN(len(sql) + 1)
		}
		return append(append(append([]byte{}, sql[:pos]...), printables[r.IntN(len(printables))]), sql[pos:]...)
	}
	pos := r.IntN(len(sql))
	if r.IntN(2) == 0 {
		return append(append([]byte{}, sql[:pos]...), sql[pos+1:]...)
	}
	out := append([]byte{}, sql...)
	out[pos] = printables[r.IntN(len(printables))]
	return out
}

func Crossover(a, b []byte) []byte {
	ai := bytes.IndexByte(a, ' ')
	bi := bytes.IndexByte(b, ' ')
	if ai < 0 || bi < 0 {
		return append([]byte{}, a...)
	}
	return append(append([]byte{}, a[:ai]...), b[bi:]...)
}

func isDelim(c byte) bool {
	return isSpace(c) || strings.ContainsRune(",();", rune(c))
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}
