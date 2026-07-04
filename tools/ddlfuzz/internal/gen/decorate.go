package gen

import (
	"bytes"
	"strings"
	"unicode/utf8"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/mutate"
)

func decorate(c *Ctx, sql string) string {
	out := []byte(sql)
	for i, n := 0, 1+c.R.IntN(3); i < n; i++ {
		switch c.R.IntN(5) {
		case 0:
			out = insertBetweenTokens(c, out, []byte(pickString(c.R, commentForms(c))))
		case 1:
			out = substituteWhitespace(c, out)
		case 2:
			out = respellIdentifier(c, out)
		case 3:
			out = respellString(c, out)
		default:
			out = wrapSpecComment(c, out)
		}
		if !utf8.Valid(out) {
			return sql
		}
	}
	return string(out)
}

func commentForms(c *Ctx) []string {
	forms := []string{"/* */", "/*+ hint */", "/*! code */", "/*!40101 code */", "/*!050001 code */", "/*!50001 code */", "-- \n", "#\n"}
	if c.IsMariaDB {
		forms = append(forms, "/*M! code */", "/*M!40101 code */")
	}
	return forms
}

func insertBetweenTokens(c *Ctx, sql []byte, insert []byte) []byte {
	toks := mutate.Tokens(sql)
	if len(toks) < 2 {
		return sql
	}
	tok := toks[1+c.R.IntN(len(toks)-1)]
	pos := bytes.Index(sql, tok)
	if pos <= 0 {
		return sql
	}
	return append(append(append([]byte{}, sql[:pos]...), append(insert, ' ')...), sql[pos:]...)
}

func substituteWhitespace(c *Ctx, sql []byte) []byte {
	spaces := bytes.Count(sql, []byte(" "))
	if spaces == 0 {
		return sql
	}
	target := c.R.IntN(spaces)
	seen := 0
	for i, b := range sql {
		if b != ' ' {
			continue
		}
		if seen == target {
			out := append([]byte{}, sql...)
			out[i] = pickString(c.R, []string{"\t", "\n", "\r", "\v", "\f"})[0]
			return out
		}
		seen++
	}
	return sql
}

func respellIdentifier(c *Ctx, sql []byte) []byte {
	toks := mutate.Tokens(sql)
	for tries := 0; tries < 20 && len(toks) > 0; tries++ {
		tok := toks[c.R.IntN(len(toks))]
		s := string(tok)
		if len(s) == 0 || strings.ContainsAny(s, "'(),;") || isKeywordish(s) {
			continue
		}
		repl := quoteMaybe(c, strings.Trim(s, "`[]\""))
		if repl != s {
			return bytes.Replace(sql, tok, []byte(repl), 1)
		}
	}
	return sql
}

func respellString(c *Ctx, sql []byte) []byte {
	toks := mutate.Tokens(sql)
	var stringsOnly [][]byte
	for _, tok := range toks {
		if len(tok) >= 2 && tok[0] == '\'' && tok[len(tok)-1] == '\'' {
			stringsOnly = append(stringsOnly, tok)
		}
	}
	if len(stringsOnly) == 0 {
		return sql
	}
	tok := stringsOnly[c.R.IntN(len(stringsOnly))]
	body := strings.Trim(string(tok[1:len(tok)-1]), "'")
	forms := []string{"N'" + body + "'", "_utf8mb4'" + body + "'", "'" + strings.ReplaceAll(body, `\`, `\\`) + "'"}
	if c.Mode&ModeNoBackslashEscapes != 0 {
		forms = append(forms, "'"+strings.ReplaceAll(body, "'", "''")+"'")
	}
	if !c.IsMariaDB {
		forms = append(forms, "$tag$"+body+"$tag$")
	}
	return bytes.Replace(sql, tok, []byte(pickString(c.R, forms)), 1)
}

func wrapSpecComment(c *Ctx, sql []byte) []byte {
	toks := mutate.Tokens(sql)
	if len(toks) == 0 {
		return sql
	}
	tok := toks[c.R.IntN(len(toks))]
	wrap := pickString(c.R, []string{"/*!", "/*!40101", "/*!50001"})
	if c.IsMariaDB && c.R.IntN(3) == 0 {
		wrap = "/*M!"
	}
	return bytes.Replace(sql, tok, []byte(wrap+" "+string(tok)+" */"), 1)
}

func isKeywordish(s string) bool {
	switch strings.ToUpper(strings.Trim(s, "`[]\"")) {
	case "ALTER", "TABLE", "ADD", "DROP", "MODIFY", "CHANGE", "COLUMN", "INDEX", "KEY", "TO", "RENAME", "SET", "DEFAULT":
		return true
	default:
		return false
	}
}
