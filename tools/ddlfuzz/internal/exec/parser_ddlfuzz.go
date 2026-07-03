//go:build ddlfuzz

package exec

import connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"

func DefaultParser(sql []byte, sqlMode uint64, isMariaDB bool) (string, error) {
	return connmysql.FuzzDDLSignature(sql, sqlMode, isMariaDB)
}
