//go:build ddlfuzz

package e2e

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
)

func pickCorpusRewrite(store *corpus.Store, ec engineConfig, rng *rand.Rand, live snapshot) (string, bool) {
	if store == nil {
		return "", false
	}
	for range 10 {
		data, ok, err := store.RandomSQL(ec.Name, rng)
		if err != nil || !ok {
			return "", false
		}
		stmt, ok := rewriteCorpusStatement(data, ec.IsMariaDB, live)
		if ok {
			return stmt, true
		}
	}
	return "", false
}

func rewriteCorpusStatement(stmt []byte, isMariaDB bool, live snapshot) (string, bool) {
	parsed, err, panicked := safeParseForE2E(stmt, 0, isMariaDB)
	if err != nil || panicked != nil || len(parsed.Stmts) == 0 {
		return "", false
	}
	if !hasActionable(parsed) {
		return "", false
	}
	shapeBefore := shapeSignature(parsed, false)
	rewritten := string(stmt)
	for _, table := range parsedTables(parsed) {
		rewritten = replaceIdentOccurrences(rewritten, table, fixtureTable)
	}
	cols := canonicalColumns(live)
	oldMap := make(map[string]string)
	newIdx := 0
	for _, name := range parsedOldColumns(parsed) {
		if name == "" || oldMap[name] != "" {
			continue
		}
		oldMap[name] = cols[newIdx%len(cols)]
		newIdx++
	}
	for old, next := range oldMap {
		rewritten = replaceIdentOccurrences(rewritten, old, next)
	}
	freshIdx := 0
	for _, name := range parsedNewColumns(parsed) {
		if name == "" || oldMap[name] != "" {
			continue
		}
		next := FreshNames[freshIdx%len(FreshNames)]
		freshIdx++
		rewritten = replaceIdentOccurrences(rewritten, name, next)
	}
	verified, err, panicked := safeParseForE2E([]byte(rewritten), 0, isMariaDB)
	if err != nil || panicked != nil || len(verified.Stmts) == 0 {
		return "", false
	}
	if shapeSignature(verified, false) != shapeBefore {
		return "", false
	}
	return rewritten, true
}

func hasActionable(p parsedStmts) bool {
	for _, stmt := range p.Stmts {
		if stmt.Kind == "alter_table" || stmt.Kind == "rename_table" {
			return true
		}
	}
	return false
}

func parsedTables(p parsedStmts) []string {
	set := map[string]bool{}
	for _, stmt := range p.Stmts {
		switch stmt.Kind {
		case "alter_table":
			if stmt.Table != "" {
				set[stmt.Table] = true
			}
		case "rename_table":
			for _, pair := range stmt.Pairs {
				if pair.OldTable != "" {
					set[pair.OldTable] = true
				}
				if pair.NewTable != "" {
					set[pair.NewTable] = true
				}
			}
		}
	}
	return sortedKeys(set)
}

func parsedOldColumns(p parsedStmts) []string {
	set := map[string]bool{}
	for _, stmt := range p.Stmts {
		for _, spec := range stmt.Specs {
			switch spec.Op {
			case "change", "drop", "rename_col":
				set[spec.OldName] = true
			}
		}
	}
	return sortedKeys(set)
}

func parsedNewColumns(p parsedStmts) []string {
	set := map[string]bool{}
	for _, stmt := range p.Stmts {
		for _, spec := range stmt.Specs {
			switch spec.Op {
			case "add", "change":
				for _, col := range spec.Cols {
					set[col.Name] = true
				}
			case "rename_col":
				set[spec.NewName] = true
			}
		}
	}
	return sortedKeys(set)
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		if k != "" {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

func shapeSignature(p parsedStmts, includeNames bool) string {
	var b strings.Builder
	for _, stmt := range p.Stmts {
		b.WriteString(stmt.Kind)
		b.WriteByte('{')
		if includeNames {
			b.WriteString(stmt.Schema)
			b.WriteByte('.')
			b.WriteString(stmt.Table)
		}
		for _, spec := range stmt.Specs {
			b.WriteByte('|')
			b.WriteString(spec.Op)
			if spec.HasPosition {
				b.WriteString("@pos")
			}
			for _, col := range spec.Cols {
				b.WriteByte(':')
				b.WriteString(col.TypeStr)
				b.WriteByte('/')
				if col.NotNull {
					b.WriteByte('N')
				}
				b.WriteByte('/')
				b.WriteString(intText(col.Precision))
				b.WriteByte(',')
				b.WriteString(intText(col.Scale))
				if includeNames {
					b.WriteByte('/')
					b.WriteString(col.Name)
				}
			}
		}
		for _, pair := range stmt.Pairs {
			b.WriteByte('|')
			if includeNames {
				b.WriteString(pair.OldTable)
				b.WriteString(">")
				b.WriteString(pair.NewTable)
			} else {
				b.WriteString("pair")
			}
		}
		b.WriteByte('}')
	}
	return b.String()
}

func intText(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func replaceIdentOccurrences(s, old, next string) string {
	if old == "" || old == next {
		return s
	}
	quotedOld := []byte(quoteIdent(old))
	quotedNext := []byte(quoteIdent(next))
	out := bytes.ReplaceAll([]byte(s), quotedOld, quotedNext)
	out = replaceWordBoundary(out, []byte(old), []byte(next))
	return string(out)
}

func replaceWordBoundary(s, old, next []byte) []byte {
	if len(old) == 0 {
		return slices.Clone(s)
	}
	var out []byte
	for i := 0; i < len(s); {
		j := bytes.Index(s[i:], old)
		if j < 0 {
			out = append(out, s[i:]...)
			break
		}
		j += i
		if identBoundary(s, j-1) && identBoundary(s, j+len(old)) {
			out = append(out, s[i:j]...)
			out = append(out, next...)
			i = j + len(old)
		} else {
			out = append(out, s[i:j+len(old)]...)
			i = j + len(old)
		}
	}
	return out
}

func identBoundary(s []byte, idx int) bool {
	if idx < 0 || idx >= len(s) {
		return true
	}
	c := s[idx]
	if c == '_' || c == '$' || c >= 0x80 || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
		return false
	}
	if c < utf8.RuneSelf {
		return true
	}
	_, size := utf8.DecodeRune(s[idx:])
	return size == 0
}

func splitTopLevelFallback(b []byte, sep byte) [][]byte {
	defer func() {
		_ = recover()
	}()
	return compare.SplitTopLevel(b, sep)
}
