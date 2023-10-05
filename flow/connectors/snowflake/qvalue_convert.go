package connsnowflake

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

var qValueKindToSnowflakeTypeMap = map[qvalue.QValueKind]string{
	qvalue.QValueKindBoolean:     "BOOLEAN",
	qvalue.QValueKindInt16:       "INTEGER",
	qvalue.QValueKindInt32:       "INTEGER",
	qvalue.QValueKindInt64:       "INTEGER",
	qvalue.QValueKindFloat32:     "FLOAT",
	qvalue.QValueKindFloat64:     "FLOAT",
	qvalue.QValueKindNumeric:     "NUMBER(38, 9)",
	qvalue.QValueKindString:      "STRING",
	qvalue.QValueKindJSON:        "VARIANT",
	qvalue.QValueKindTimestamp:   "TIMESTAMP_NTZ",
	qvalue.QValueKindTimestampTZ: "TIMESTAMP_TZ",
	qvalue.QValueKindTime:        "TIME",
	qvalue.QValueKindDate:        "DATE",
	qvalue.QValueKindBit:         "BINARY",
	qvalue.QValueKindBytes:       "BINARY",
	qvalue.QValueKindStruct:      "STRING",
	qvalue.QValueKindUUID:        "STRING",
	qvalue.QValueKindTimeTZ:      "STRING",
	qvalue.QValueKindInvalid:     "STRING",
	qvalue.QValueKindHStore:      "STRING",

	// array types will be mapped to STRING
	qvalue.QValueKindArrayFloat32: "VARIANT",
	qvalue.QValueKindArrayFloat64: "VARIANT",
	qvalue.QValueKindArrayInt32:   "VARIANT",
	qvalue.QValueKindArrayInt64:   "VARIANT",
	qvalue.QValueKindArrayString:  "VARIANT",
}

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
}

func qValueKindToSnowflakeType(colType qvalue.QValueKind) string {
	if val, ok := qValueKindToSnowflakeTypeMap[colType]; ok {
		return val
	}
	return "STRING"
}

func snowflakeTypeToQValueKind(name string) (qvalue.QValueKind, error) {
	if val, ok := snowflakeTypeToQValueKindMap[name]; ok {
		return val, nil
	}
	return "", fmt.Errorf("unsupported database type name: %s", name)
}
