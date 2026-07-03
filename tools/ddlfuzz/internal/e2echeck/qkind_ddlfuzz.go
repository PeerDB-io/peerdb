//go:build ddlfuzz

package e2echeck

import connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"

func qkindString(typeStr string) string {
	kind, err := connmysql.QkindFromMysqlColumnType(typeStr, true, 0)
	if err != nil {
		return "ERR"
	}
	return string(kind)
}
