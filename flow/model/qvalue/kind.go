package qvalue

import (
	"context"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

type QValueKind string

const (
	QValueKindInvalid     QValueKind = "invalid"
	QValueKindFloat32     QValueKind = "float32"
	QValueKindFloat64     QValueKind = "float64"
	QValueKindInt16       QValueKind = "int16"
	QValueKindInt32       QValueKind = "int32"
	QValueKindInt64       QValueKind = "int64"
	QValueKindBoolean     QValueKind = "bool"
	QValueKindStruct      QValueKind = "struct"
	QValueKindQChar       QValueKind = "qchar"
	QValueKindString      QValueKind = "string"
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
	QValueKindInt16:       "INTEGER",
	QValueKindInt32:       "INTEGER",
	QValueKindInt64:       "INTEGER",
	QValueKindFloat32:     "FLOAT",
	QValueKindFloat64:     "FLOAT",
	QValueKindQChar:       "CHAR",
	QValueKindString:      "STRING",
	QValueKindJSON:        "VARIANT",
	QValueKindJSONB:       "VARIANT",
	QValueKindTimestamp:   "TIMESTAMP_NTZ",
	QValueKindTimestampTZ: "TIMESTAMP_TZ",
	QValueKindInterval:    "VARIANT",
	QValueKindTime:        "TIME",
	QValueKindTimeTZ:      "TIME",
	QValueKindDate:        "DATE",
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
	QValueKindArrayJSON:        "VARIANT",
	QValueKindArrayJSONB:       "VARIANT",
	QValueKindArrayUUID:        "VARIANT",
}

var QValueKindToClickHouseTypeMap = map[QValueKind]string{
	QValueKindBoolean:     "Bool",
	QValueKindInt16:       "Int16",
	QValueKindInt32:       "Int32",
	QValueKindInt64:       "Int64",
	QValueKindFloat32:     "Float32",
	QValueKindFloat64:     "Float64",
	QValueKindQChar:       "FixedString(1)",
	QValueKindString:      "String",
	QValueKindJSON:        "String",
	QValueKindTimestamp:   "DateTime64(6)",
	QValueKindTimestampTZ: "DateTime64(6)",
	QValueKindTSTZRange:   "String",
	QValueKindTime:        "DateTime64(6)",
	QValueKindTimeTZ:      "DateTime64(6)",
	QValueKindDate:        "Date32",
	QValueKindBytes:       "String",
	QValueKindStruct:      "String",
	QValueKindUUID:        "UUID",
	QValueKindInvalid:     "String",
	QValueKindHStore:      "String",

	QValueKindArrayFloat32:     "Array(Float32)",
	QValueKindArrayFloat64:     "Array(Float64)",
	QValueKindArrayInt32:       "Array(Int32)",
	QValueKindArrayInt64:       "Array(Int64)",
	QValueKindArrayString:      "Array(String)",
	QValueKindArrayBoolean:     "Array(Bool)",
	QValueKindArrayInt16:       "Array(Int16)",
	QValueKindArrayDate:        "Array(Date)",
	QValueKindArrayTimestamp:   "Array(DateTime64(6))",
	QValueKindArrayTimestampTZ: "Array(DateTime64(6))",
	QValueKindArrayJSON:        "String",
	QValueKindArrayJSONB:       "String",
	QValueKindArrayUUID:        "Array(UUID)",
}

func getClickHouseTypeForNumericColumn(ctx context.Context, env map[string]string, column *protos.FieldDescription) (string, error) {
	if column.TypeModifier == -1 {
		numericAsStringEnabled, err := peerdbenv.PeerDBEnableClickHouseNumericAsString(ctx, env)
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

// SEE ALSO: QField ToDWHColumnType
func (kind QValueKind) ToDWHColumnType(ctx context.Context, env map[string]string, dwhType protos.DBType, column *protos.FieldDescription,
) (string, error) {
	switch dwhType {
	case protos.DBType_SNOWFLAKE:
		if kind == QValueKindNumeric {
			precision, scale := datatypes.GetNumericTypeForWarehouse(column.TypeModifier, datatypes.SnowflakeNumericCompatibility{})
			return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale), nil
		} else if val, ok := QValueKindToSnowflakeTypeMap[kind]; ok {
			return val, nil
		} else {
			return "STRING", nil
		}
	case protos.DBType_CLICKHOUSE:
		if kind == QValueKindNumeric {
			return getClickHouseTypeForNumericColumn(ctx, env, column)
		} else if val, ok := QValueKindToClickHouseTypeMap[kind]; ok {
			return val, nil
		} else {
			return "String", nil
		}
	default:
		return "", fmt.Errorf("unknown dwh type: %v", dwhType)
	}
}
