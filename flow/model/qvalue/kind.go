package qvalue

import (
	"fmt"
	"strings"
)

type QValueKind string

const (
	QValueKindEmpty       QValueKind = ""
	QValueKindInvalid     QValueKind = "invalid"
	QValueKindFloat32     QValueKind = "float32"
	QValueKindFloat64     QValueKind = "float64"
	QValueKindInt16       QValueKind = "int16"
	QValueKindInt32       QValueKind = "int32"
	QValueKindInt64       QValueKind = "int64"
	QValueKindBoolean     QValueKind = "bool"
	QValueKindStruct      QValueKind = "struct"
	QValueKindString      QValueKind = "string"
	QValueKindTimestamp   QValueKind = "timestamp"
	QValueKindTimestampTZ QValueKind = "timestamptz"
	QValueKindDate        QValueKind = "date"
	QValueKindTime        QValueKind = "time"
	QValueKindTimeTZ      QValueKind = "timetz"
	QValueKindNumeric     QValueKind = "numeric"
	QValueKindBytes       QValueKind = "bytes"
	QValueKindUUID        QValueKind = "uuid"
	QValueKindJSON        QValueKind = "json"
	QValueKindBit         QValueKind = "bit"
	QValueKindHStore      QValueKind = "hstore"
	QValueKindGeography   QValueKind = "geography"
	QValueKindGeometry    QValueKind = "geometry"
	QValueKindPoint       QValueKind = "point"

	// network types
	QValueKindCIDR    QValueKind = "cidr"
	QValueKindINET    QValueKind = "inet"
	QValueKindMacaddr QValueKind = "macaddr"

	// array types
	QValueKindArrayFloat32     QValueKind = "array_float32"
	QValueKindArrayFloat64     QValueKind = "array_float64"
	QValueKindArrayInt16       QValueKind = "array_int16"
	QValueKindArrayInt32       QValueKind = "array_int32"
	QValueKindArrayInt64       QValueKind = "array_int64"
	QValueKindArrayString      QValueKind = "array_string"
	QValueKindArrayDate        QValueKind = "array_date"
	QValueKindArrayTimestamp   QValueKind = "array_timestamp"
	QValueKindArrayTimestampTZ QValueKind = "array_timestamptz"
	QValueKindArrayBoolean     QValueKind = "array_bool"
)

func (kind QValueKind) IsArray() bool {
	return strings.HasPrefix(string(kind), "array_")
}

var QValueKindToSnowflakeTypeMap = map[QValueKind]string{
	QValueKindBoolean:     "BOOLEAN",
	QValueKindInt16:       "INTEGER",
	QValueKindInt32:       "INTEGER",
	QValueKindInt64:       "INTEGER",
	QValueKindFloat32:     "FLOAT",
	QValueKindFloat64:     "FLOAT",
	QValueKindNumeric:     "NUMBER(38, 9)",
	QValueKindString:      "STRING",
	QValueKindJSON:        "VARIANT",
	QValueKindTimestamp:   "TIMESTAMP_NTZ",
	QValueKindTimestampTZ: "TIMESTAMP_TZ",
	QValueKindTime:        "TIME",
	QValueKindTimeTZ:      "TIME",
	QValueKindDate:        "DATE",
	QValueKindBit:         "BINARY",
	QValueKindBytes:       "BINARY",
	QValueKindStruct:      "STRING",
	QValueKindUUID:        "STRING",
	QValueKindInvalid:     "STRING",
	QValueKindHStore:      "VARIANT",
	QValueKindGeography:   "GEOGRAPHY",
	QValueKindGeometry:    "GEOMETRY",
	QValueKindPoint:       "GEOMETRY",

	// array types will be mapped to VARIANT
	QValueKindArrayFloat32:     "VARIANT",
	QValueKindArrayFloat64:     "VARIANT",
	QValueKindArrayInt32:       "VARIANT",
	QValueKindArrayInt64:       "VARIANT",
	QValueKindArrayInt16:       "VARIANT",
	QValueKindArrayString:      "VARIANT",
	QValueKindArrayDate:        "VARIANT",
	QValueKindArrayTimestamp:   "VARIANT",
	QValueKindArrayTimestampTZ: "VARIANT",
	QValueKindArrayBoolean:     "VARIANT",
}

var QValueKindToClickhouseTypeMap = map[QValueKind]string{
	QValueKindBoolean:     "Bool",
	QValueKindInt16:       "Int16",
	QValueKindInt32:       "Int32",
	QValueKindInt64:       "Int64",
	QValueKindFloat32:     "Float32",
	QValueKindFloat64:     "Float64",
	QValueKindNumeric:     "Float64",
	QValueKindString:      "String",
	QValueKindJSON:        "String",
	QValueKindTimestamp:   "DateTime64(6)",
	QValueKindTimestampTZ: "TIMESTAMP",
	QValueKindTime:        "TIME",
	QValueKindDate:        "DATE",
	QValueKindBit:         "Boolean",
	QValueKindBytes:       "String",
	QValueKindStruct:      "String",
	QValueKindUUID:        "String",
	QValueKindTimeTZ:      "String",
	QValueKindInvalid:     "String",
	QValueKindHStore:      "String",
	QValueKindGeography:   "GEOGRAPHY",
	QValueKindGeometry:    "GEOMETRY",
	QValueKindPoint:       "GEOMETRY",

	// array types will be mapped to VARIANT
	QValueKindArrayFloat32: "Array(Float32)",
	QValueKindArrayFloat64: "Array(Float64)",
	QValueKindArrayInt32:   "Array(Int32)",
	QValueKindArrayInt64:   "Array(Int64)",
	QValueKindArrayString:  "Array(String)",
}

func (kind QValueKind) ToDWHColumnType(dwhType QDWHType) (string, error) {
	if dwhType != QDWHTypeSnowflake {
		return "", fmt.Errorf("unsupported DWH type: %v", dwhType)
	}

	if val, ok := QValueKindToSnowflakeTypeMap[kind]; ok {
		return val, nil
	} else {
		return "STRING", nil
	}
}
