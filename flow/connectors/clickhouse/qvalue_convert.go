package connclickhouse

import (
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func qValueKindToClickhouseType(colType qvalue.QValueKind) (string, error) {
	val, err := colType.ToDWHColumnType(qvalue.QDWHTypeClickhouse)
	if err != nil {
		return "", err
	}

	return val, err
}
