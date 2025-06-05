package connsnowflake

import (
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var snowflakeTypeToQValueKindMap = map[string]types.QValueKind{
	"INT":           types.QValueKindInt32,
	"BIGINT":        types.QValueKindInt64,
	"FLOAT":         types.QValueKindFloat64,
	"DOUBLE":        types.QValueKindFloat64,
	"REAL":          types.QValueKindFloat64,
	"VARCHAR":       types.QValueKindString,
	"CHAR":          types.QValueKindString,
	"TEXT":          types.QValueKindString,
	"BOOLEAN":       types.QValueKindBoolean,
	"DATETIME":      types.QValueKindTimestamp,
	"TIMESTAMP":     types.QValueKindTimestamp,
	"TIMESTAMP_NTZ": types.QValueKindTimestamp,
	"TIMESTAMP_TZ":  types.QValueKindTimestampTZ,
	"TIME":          types.QValueKindTime,
	"DATE":          types.QValueKindDate,
	"BLOB":          types.QValueKindBytes,
	"BYTEA":         types.QValueKindBytes,
	"BINARY":        types.QValueKindBytes,
	"FIXED":         types.QValueKindNumeric,
	"NUMBER":        types.QValueKindNumeric,
	"DECIMAL":       types.QValueKindNumeric,
	"NUMERIC":       types.QValueKindNumeric,
	"VARIANT":       types.QValueKindJSON,
	"GEOMETRY":      types.QValueKindGeometry,
	"GEOGRAPHY":     types.QValueKindGeography,
}

func snowflakeTypeToQValueKind(name string) (types.QValueKind, error) {
	if val, ok := snowflakeTypeToQValueKindMap[name]; ok {
		return val, nil
	}
	return "", fmt.Errorf("unsupported database type name: %s", name)
}
