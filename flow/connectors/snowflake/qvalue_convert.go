package connsnowflake

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

var snowflakeTypeToQValueKindMap = map[string]qvalue.QKind{
	"INT":           qvalue.QKindInt32,
	"BIGINT":        qvalue.QKindInt64,
	"FLOAT":         qvalue.QKindFloat64,
	"DOUBLE":        qvalue.QKindFloat64,
	"REAL":          qvalue.QKindFloat64,
	"VARCHAR":       qvalue.QKindString,
	"CHAR":          qvalue.QKindString,
	"TEXT":          qvalue.QKindString,
	"BOOLEAN":       qvalue.QKindBoolean,
	"DATETIME":      qvalue.QKindTimestamp,
	"TIMESTAMP":     qvalue.QKindTimestamp,
	"TIMESTAMP_NTZ": qvalue.QKindTimestamp,
	"TIMESTAMP_TZ":  qvalue.QKindTimestampTZ,
	"TIME":          qvalue.QKindTime,
	"DATE":          qvalue.QKindDate,
	"BLOB":          qvalue.QKindBytes,
	"BYTEA":         qvalue.QKindBytes,
	"BINARY":        qvalue.QKindBytes,
	"FIXED":         qvalue.QKindNumeric,
	"NUMBER":        qvalue.QKindNumeric,
	"DECIMAL":       qvalue.QKindNumeric,
	"NUMERIC":       qvalue.QKindNumeric,
	"VARIANT":       qvalue.QKindJSON,
	"GEOMETRY":      qvalue.QKindGeometry,
	"GEOGRAPHY":     qvalue.QKindGeography,
}

func snowflakeTypeToQValueKind(name string) (qvalue.QKind, error) {
	if val, ok := snowflakeTypeToQValueKindMap[name]; ok {
		return val, nil
	}
	return qvalue.QKindInvalid, fmt.Errorf("unsupported database type name: %s", name)
}
