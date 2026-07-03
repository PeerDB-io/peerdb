package e2echeck

import connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"

// qkindString mirrors the live lane's rendering: QkindFromMysqlColumnType is
// untagged production code, so both builds share the one implementation.
func qkindString(typeStr string) string {
	kind, err := connmysql.QkindFromMysqlColumnType(typeStr, true, 0)
	if err != nil {
		return "ERR"
	}
	return string(kind)
}
