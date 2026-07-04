package dict

import (
	"embed"
	"strings"
	"sync"
)

//go:embed mysql.txt mariadb.txt
var files embed.FS

var (
	mysqlTokens   = sync.OnceValue(func() []string { return parse("mysql.txt") })
	mariadbTokens = sync.OnceValue(func() []string { return parse("mariadb.txt") })
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
