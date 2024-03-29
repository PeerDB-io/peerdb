package qvalue

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type (
	QKind int8
	QType struct {
		Kind  QKind
		Array uint8
	}
)

const (
	QKindInvalid QKind = iota
	QKindFloat32
	QKindFloat64
	QKindInt16
	QKindInt32
	QKindInt64
	QKindBoolean
	QKindStruct
	QKindQChar
	QKindString
	QKindTimestamp
	QKindTimestampTZ
	QKindDate
	QKindTime
	QKindTimeTZ
	QKindInterval
	QKindNumeric
	QKindBytes
	QKindUUID
	QKindJSON
	QKindBit
	QKindHStore
	QKindGeography
	QKindGeometry
	QKindPoint

	// network types
	QKindCIDR
	QKindINET
	QKindMacaddr
)

func ParseQType(s string) QType {
	if array, ok := strings.CutPrefix(s, "array_"); ok {
		atype := ParseQType(array)
		atype.Array += 1
		return atype
	}

	return QType{Kind: ParseQKind(s)}
}

func ParseQKind(s string) QKind {
	switch s {
	case "float32":
		return QKindFloat32
	case "float64":
		return QKindFloat64
	case "int16":
		return QKindInt16
	case "int32":
		return QKindInt32
	case "int64":
		return QKindInt64
	case "bool":
		return QKindBoolean
	case "struct":
		return QKindStruct
	case "qchar":
		return QKindQChar
	case "string":
		return QKindString
	case "timestamp":
		return QKindTimestamp
	case "timestamptz":
		return QKindTimestampTZ
	case "date":
		return QKindDate
	case "time":
		return QKindTime
	case "timetz":
		return QKindTimeTZ
	case "interval":
		return QKindInterval
	case "numeric":
		return QKindNumeric
	case "bytes":
		return QKindBytes
	case "uuid":
		return QKindUUID
	case "json":
		return QKindJSON
	case "bit":
		return QKindBit
	case "hstore":
		return QKindHStore
	case "geography":
		return QKindGeography
	case "geometry":
		return QKindGeometry
	case "point":
		return QKindPoint
		// network types
	case "cidr":
		return QKindCIDR
	case "inet":
		return QKindINET
	case "macaddr":
		return QKindMacaddr
	default:
		return QKindInvalid
	}
}

func (qt QType) String() string {
	if qt.Array > 0 {
		return "array_" + QType{Kind: qt.Kind, Array: qt.Array - 1}.String()
	}
	switch qt.Kind {
	case QKindInvalid:
		return "invalid"
	case QKindFloat32:
		return "float32"
	case QKindFloat64:
		return "float64"
	case QKindInt16:
		return "int16"
	case QKindInt32:
		return "int32"
	case QKindInt64:
		return "int64"
	case QKindBoolean:
		return "bool"
	case QKindStruct:
		return "struct"
	case QKindQChar:
		return "qchar"
	case QKindString:
		return "string"
	case QKindTimestamp:
		return "timestamp"
	case QKindTimestampTZ:
		return "timestamptz"
	case QKindDate:
		return "date"
	case QKindTime:
		return "time"
	case QKindTimeTZ:
		return "timetz"
	case QKindInterval:
		return "interval"
	case QKindNumeric:
		return "numeric"
	case QKindBytes:
		return "bytes"
	case QKindUUID:
		return "uuid"
	case QKindJSON:
		return "json"
	case QKindBit:
		return "bit"
	case QKindHStore:
		return "hstore"
	case QKindGeography:
		return "geography"
	case QKindGeometry:
		return "geometry"
	case QKindPoint:
		return "point"
		// network types
	case QKindCIDR:
		return "cidr"
	case QKindINET:
		return "inet"
	case QKindMacaddr:
		return "macaddr"
	default:
		return ""
	}
}

func (qt QType) IsArray() bool {
	return qt.Array > 0
}

var QKindToSnowflakeTypeMap = map[QKind]string{
	QKindBoolean:     "BOOLEAN",
	QKindInt16:       "INTEGER",
	QKindInt32:       "INTEGER",
	QKindInt64:       "INTEGER",
	QKindFloat32:     "FLOAT",
	QKindFloat64:     "FLOAT",
	QKindNumeric:     "NUMBER(38, 9)",
	QKindQChar:       "CHAR",
	QKindString:      "STRING",
	QKindJSON:        "VARIANT",
	QKindTimestamp:   "TIMESTAMP_NTZ",
	QKindTimestampTZ: "TIMESTAMP_TZ",
	QKindInterval:    "VARIANT",
	QKindTime:        "TIME",
	QKindTimeTZ:      "TIME",
	QKindDate:        "DATE",
	QKindBit:         "BINARY",
	QKindBytes:       "BINARY",
	QKindStruct:      "STRING",
	QKindUUID:        "STRING",
	QKindInvalid:     "STRING",
	QKindHStore:      "VARIANT",
	QKindGeography:   "GEOGRAPHY",
	QKindGeometry:    "GEOMETRY",
	QKindPoint:       "GEOMETRY",
}

var QKindToClickhouseTypeMap = map[QKind]string{
	QKindBoolean:     "Bool",
	QKindInt16:       "Int16",
	QKindInt32:       "Int32",
	QKindInt64:       "Int64",
	QKindFloat32:     "Float32",
	QKindFloat64:     "Float64",
	QKindNumeric:     "Decimal128(9)",
	QKindQChar:       "FixedString(1)",
	QKindString:      "String",
	QKindJSON:        "String",
	QKindTimestamp:   "DateTime64(6)",
	QKindTimestampTZ: "DateTime64(6)",
	QKindTime:        "String",
	QKindDate:        "Date",
	QKindBit:         "Boolean",
	QKindBytes:       "String",
	QKindStruct:      "String",
	QKindUUID:        "UUID",
	QKindTimeTZ:      "String",
	QKindInvalid:     "String",
	QKindHStore:      "String",
}

func (qt QType) ToDWHColumnType(dwhType protos.DBType) (string, error) {
	switch dwhType {
	case protos.DBType_SNOWFLAKE:
		if qt.Array > 0 {
			return "VARIANT", nil
		} else if val, ok := QKindToSnowflakeTypeMap[qt.Kind]; ok {
			return val, nil
		} else {
			return "STRING", nil
		}
	case protos.DBType_CLICKHOUSE:
		if qt.Array > 0 {
			akind, err := QType{Kind: qt.Kind, Array: qt.Array - 1}.ToDWHColumnType(dwhType)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("Array(%s)", akind), nil
		} else if val, ok := QKindToClickhouseTypeMap[qt.Kind]; ok {
			return val, nil
		} else {
			return "String", nil
		}
	default:
		return "", fmt.Errorf("unknown dwh type: %v", dwhType)
	}
}
