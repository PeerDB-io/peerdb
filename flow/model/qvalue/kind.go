package qvalue

import (
	"context"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

type QValueKind string

const (
	QValueKindInvalid     QValueKind = "invalid"
	QValueKindFloat32     QValueKind = "float32"
	QValueKindFloat64     QValueKind = "float64"
	QValueKindInt8        QValueKind = "int8"
	QValueKindInt16       QValueKind = "int16"
	QValueKindInt32       QValueKind = "int32"
	QValueKindInt64       QValueKind = "int64"
	QValueKindUInt8       QValueKind = "uint8"
	QValueKindUInt16      QValueKind = "uint16"
	QValueKindUInt32      QValueKind = "uint32"
	QValueKindUInt64      QValueKind = "uint64"
	QValueKindBoolean     QValueKind = "bool"
	QValueKindQChar       QValueKind = "qchar"
	QValueKindString      QValueKind = "string"
	QValueKindEnum        QValueKind = "enum"
	QValueKindTimestamp   QValueKind = "timestamp"
	QValueKindTimestampTZ QValueKind = "timestamptz"
	QValueKindDate        QValueKind = "date"
	QValueKindTime        QValueKind = "time"
	QValueKindTimeTZ      QValueKind = "timetz"
	QValueKindInterval    QValueKind = "interval"
	QValueKindTSTZRange   QValueKind = "tstzrange"
	QValueKindNumeric     QValueKind = "numeric"
	QValueKindBytes       QValueKind = "bytes"
	QValueKindUUID        QValueKind = "uuid"
	QValueKindJSON        QValueKind = "json"
	QValueKindJSONB       QValueKind = "jsonb"
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
	QValueKindArrayEnum        QValueKind = "array_enum"
	QValueKindArrayDate        QValueKind = "array_date"
	QValueKindArrayTimestamp   QValueKind = "array_timestamp"
	QValueKindArrayTimestampTZ QValueKind = "array_timestamptz"
	QValueKindArrayBoolean     QValueKind = "array_bool"
	QValueKindArrayJSON        QValueKind = "array_json"
	QValueKindArrayJSONB       QValueKind = "array_jsonb"
	QValueKindArrayUUID        QValueKind = "array_uuid"
)

func (kind QValueKind) IsArray() bool {
	return strings.HasPrefix(string(kind), "array_")
}

var QValueKindToSnowflakeTypeMap = map[QValueKind]string{
	QValueKindBoolean:     "BOOLEAN",
	QValueKindInt8:        "INTEGER",
	QValueKindInt16:       "INTEGER",
	QValueKindInt32:       "INTEGER",
	QValueKindInt64:       "INTEGER",
	QValueKindUInt8:       "INTEGER",
	QValueKindUInt16:      "INTEGER",
	QValueKindUInt32:      "INTEGER",
	QValueKindUInt64:      "INTEGER",
	QValueKindFloat32:     "FLOAT",
	QValueKindFloat64:     "FLOAT",
	QValueKindQChar:       "CHAR",
	QValueKindString:      "STRING",
	QValueKindEnum:        "STRING",
	QValueKindJSON:        "VARIANT",
	QValueKindJSONB:       "VARIANT",
	QValueKindTimestamp:   "TIMESTAMP_NTZ",
	QValueKindTimestampTZ: "TIMESTAMP_TZ",
	QValueKindInterval:    "VARIANT",
	QValueKindTime:        "TIME",
	QValueKindTimeTZ:      "TIME",
	QValueKindDate:        "DATE",
	QValueKindBytes:       "BINARY",
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
	QValueKindArrayEnum:        "VARIANT",
	QValueKindArrayDate:        "VARIANT",
	QValueKindArrayTimestamp:   "VARIANT",
	QValueKindArrayTimestampTZ: "VARIANT",
	QValueKindArrayBoolean:     "VARIANT",
	QValueKindArrayJSON:        "VARIANT",
	QValueKindArrayJSONB:       "VARIANT",
	QValueKindArrayUUID:        "VARIANT",
}

var QValueKindToClickHouseTypeMap = map[QValueKind]string{
	QValueKindBoolean:     "Bool",
	QValueKindInt8:        "Int8",
	QValueKindInt16:       "Int16",
	QValueKindInt32:       "Int32",
	QValueKindInt64:       "Int64",
	QValueKindUInt8:       "UInt8",
	QValueKindUInt16:      "UInt16",
	QValueKindUInt32:      "UInt32",
	QValueKindUInt64:      "UInt64",
	QValueKindFloat32:     "Float32",
	QValueKindFloat64:     "Float64",
	QValueKindQChar:       "FixedString(1)",
	QValueKindString:      "String",
	QValueKindEnum:        "LowCardinality(String)",
	QValueKindJSON:        "String",
	QValueKindTimestamp:   "DateTime64(6)",
	QValueKindTimestampTZ: "DateTime64(6)",
	QValueKindTSTZRange:   "String",
	QValueKindTime:        "DateTime64(6)",
	QValueKindTimeTZ:      "DateTime64(6)",
	QValueKindDate:        "Date32",
	QValueKindBytes:       "String",
	QValueKindUUID:        "UUID",
	QValueKindInvalid:     "String",
	QValueKindHStore:      "String",

	QValueKindArrayFloat32:     "Array(Float32)",
	QValueKindArrayFloat64:     "Array(Float64)",
	QValueKindArrayInt16:       "Array(Int16)",
	QValueKindArrayInt32:       "Array(Int32)",
	QValueKindArrayInt64:       "Array(Int64)",
	QValueKindArrayString:      "Array(String)",
	QValueKindArrayEnum:        "Array(LowCardinality(String))",
	QValueKindArrayBoolean:     "Array(Bool)",
	QValueKindArrayDate:        "Array(Date)",
	QValueKindArrayTimestamp:   "Array(DateTime64(6))",
	QValueKindArrayTimestampTZ: "Array(DateTime64(6))",
	QValueKindArrayJSON:        "String",
	QValueKindArrayJSONB:       "String",
	QValueKindArrayUUID:        "Array(UUID)",
}

func getClickHouseTypeForNumericColumn(ctx context.Context, env map[string]string, column *protos.FieldDescription) (string, error) {
	if column.TypeModifier == -1 {
		numericAsStringEnabled, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
		if err != nil {
			return "", err
		}
		if numericAsStringEnabled {
			return "String", nil
		}
	} else if rawPrecision, _ := datatypes.ParseNumericTypmod(column.TypeModifier); rawPrecision > datatypes.PeerDBClickHouseMaxPrecision {
		return "String", nil
	}
	precision, scale := datatypes.GetNumericTypeForWarehouse(column.TypeModifier, datatypes.ClickHouseNumericCompatibility{})
	return fmt.Sprintf("Decimal(%d, %d)", precision, scale), nil
}

func (kind QValueKind) ToDWHColumnType(
	ctx context.Context, env map[string]string, dwhType protos.DBType, column *protos.FieldDescription, nullableEnabled bool,
) (string, error) {
	var colType string
	switch dwhType {
	case protos.DBType_SNOWFLAKE:
		if kind == QValueKindNumeric {
			precision, scale := datatypes.GetNumericTypeForWarehouse(column.TypeModifier, datatypes.SnowflakeNumericCompatibility{})
			colType = fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
		} else if val, ok := QValueKindToSnowflakeTypeMap[kind]; ok {
			colType = val
		} else {
			colType = "STRING"
		}
		if nullableEnabled && !column.Nullable {
			colType += " NOT NULL"
		}
	case protos.DBType_CLICKHOUSE:
		if kind == QValueKindNumeric {
			var err error
			colType, err = getClickHouseTypeForNumericColumn(ctx, env, column)
			if err != nil {
				return "", err
			}
		} else if val, ok := QValueKindToClickHouseTypeMap[kind]; ok {
			colType = val
		} else {
			colType = "String"
		}
		if nullableEnabled && column.Nullable && !kind.IsArray() {
			if colType == "LowCardinality(String)" {
				colType = "LowCardinality(Nullable(String))"
			} else {
				colType = fmt.Sprintf("Nullable(%s)", colType)
			}
		}
	default:
		return "", fmt.Errorf("unknown dwh type: %v", dwhType)
	}
	return colType, nil
}
