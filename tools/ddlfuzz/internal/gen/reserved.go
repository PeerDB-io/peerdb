package gen

import (
	_ "embed"
	"strings"
	"sync"
)

//go:embed reserved_mysql.txt
var reservedMySQLText string

//go:embed reserved_mariadb.txt
var reservedMariaDBText string

var (
	reservedMySQL   = sync.OnceValue(func() map[string]struct{} { return parseReserved(reservedMySQLText) })
	reservedMariaDB = sync.OnceValue(func() map[string]struct{} { return parseReserved(reservedMariaDBText) })
)

func parseReserved(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, line := range strings.Fields(s) {
		out[asciiLower(line)] = struct{}{}
	}
	return out
}

func isReservedIdent(c *Ctx, ident string) bool {
	set := reservedMySQL()
	if c != nil && c.IsMariaDB {
		set = reservedMariaDB()
	}
	_, ok := set[asciiLower(ident)]
	return ok
}

func asciiLower(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b.WriteByte(c)
	}
	return b.String()
}
