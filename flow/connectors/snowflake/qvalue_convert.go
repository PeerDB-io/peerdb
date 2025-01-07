package connsnowflake

import (
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

var snowflakeTypeToQValueKindMap = map[string]qvalue.QValueKind{
	"INT":           qvalue.QValueKindInt32,
	"BIGINT":        qvalue.QValueKindInt64,
	"FLOAT":         qvalue.QValueKindFloat64,
	"DOUBLE":        qvalue.QValueKindFloat64,
	"REAL":          qvalue.QValueKindFloat64,
	"VARCHAR":       qvalue.QValueKindString,
	"CHAR":          qvalue.QValueKindString,
	"TEXT":          qvalue.QValueKindString,
	"BOOLEAN":       qvalue.QValueKindBoolean,
	"DATETIME":      qvalue.QValueKindTimestamp,
	"TIMESTAMP":     qvalue.QValueKindTimestamp,
	"TIMESTAMP_NTZ": qvalue.QValueKindTimestamp,
	"TIMESTAMP_TZ":  qvalue.QValueKindTimestampTZ,
	"TIME":          qvalue.QValueKindTime,
	"DATE":          qvalue.QValueKindDate,
	"BLOB":          qvalue.QValueKindBytes,
	"BYTEA":         qvalue.QValueKindBytes,
	"BINARY":        qvalue.QValueKindBytes,
	"FIXED":         qvalue.QValueKindNumeric,
	"NUMBER":        qvalue.QValueKindNumeric,
	"DECIMAL":       qvalue.QValueKindNumeric,
	"NUMERIC":       qvalue.QValueKindNumeric,
	"VARIANT":       qvalue.QValueKindJSON,
	"GEOMETRY":      qvalue.QValueKindGeometry,
	"GEOGRAPHY":     qvalue.QValueKindGeography,
}

func snowflakeTypeToQValueKind(name string) (qvalue.QValueKind, error) {
	if val, ok := snowflakeTypeToQValueKindMap[name]; ok {
		return val, nil
	}
	return "", fmt.Errorf("unsupported database type name: %s", name)
}
