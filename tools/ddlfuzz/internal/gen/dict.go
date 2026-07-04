package gen

import "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/dict"

func keywordTokens(isMariaDB bool) []string {
	engine := "mysql"
	if isMariaDB {
		engine = "mariadb"
	}
	toks := dict.Tokens(engine)
	if len(toks) == 0 {
		return []string{"first", "after", "period", "system", "vector", "column", "table", "select"}
	}
	return toks
}
