package dict

import (
	"embed"
	"strings"
)

//go:embed mysql.txt mariadb.txt
var files embed.FS

func Tokens(engine string) []string {
	name := "mysql.txt"
	if engine == "mariadb" || engine == "maria" {
		name = "mariadb.txt"
	}
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
