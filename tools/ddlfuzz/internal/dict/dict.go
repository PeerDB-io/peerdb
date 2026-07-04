package dict

import (
	"embed"
	"strings"
	"sync"
)

//go:embed mysql.txt mariadb.txt
var files embed.FS

var (
	mysqlTokens   = sync.OnceValue(func() []string { return appendExtra(parse("mysql.txt"), mysqlExtra...) })
	mariadbTokens = sync.OnceValue(func() []string { return appendExtra(parse("mariadb.txt"), mariadbExtra...) })
)

func Tokens(engine string) []string {
	if engine == "mariadb" || engine == "maria" {
		return mariadbTokens()
	}
	return mysqlTokens()
}

func parse(name string) []string {
	b, err := files.ReadFile(name)
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func appendExtra(base []string, extra ...string) []string {
	out := append([]string(nil), base...)
	seen := map[string]struct{}{}
	for _, tok := range out {
		seen[tok] = struct{}{}
	}
	for _, tok := range extra {
		if _, ok := seen[tok]; ok {
			continue
		}
		seen[tok] = struct{}{}
		out = append(out, tok)
	}
	return out
}

var commonExtra = []string{
	"/*",
	"*/",
	"/*!",
	"/*+",
	"/*!90699",
	"/*!90700",
	"/*!90701",
	"/*!090699",
	"/*!090700",
	"/*!090701",
	"/*!99999",
	"-- ",
	"#",
	"'",
	"`",
	"\"",
	";",
	"REMOVE PARTITIONING",
	"WAIT 3",
	"NOWAIT",
	"IF NOT EXISTS",
}

var mysqlExtra = commonExtra

var mariadbExtra = append(append([]string{}, commonExtra...),
	"/*M!",
	"/*M!13009",
	"/*M!13010",
	"/*M!13011",
	"/*M!130099",
	"/*M!130100",
	"/*M!130101",
	"/*!130100",
	"/*!130101",
)
