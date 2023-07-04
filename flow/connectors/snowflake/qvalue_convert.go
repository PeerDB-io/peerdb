package connsnowflake

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func qValueKindToSnowflakeType(colType string) string {
	switch qvalue.QValueKind(colType) {
	case qvalue.QValueKindBoolean:
		return "BOOLEAN"
	// integer types
	case qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64:
		return "INTEGER"
	// decimal types
	// The names FLOAT, FLOAT4, and FLOAT8 are for compatibility with other systems
	// Snowflake treats all three as 64-bit floating-point numbers.
	case qvalue.QValueKindFloat32, qvalue.QValueKindFloat64:
		return "FLOAT"
	case qvalue.QValueKindNumeric:
		return "NUMBER"
	// string related STRING , TEXT , NVARCHAR ,
	// NVARCHAR2 , CHAR VARYING , NCHAR VARYING
	//Synonymous with VARCHAR.
	case qvalue.QValueKindString:
		return "STRING"
	// json also is stored as string for now
	case qvalue.QValueKindJSON:
		return "STRING"
	// time related
	case qvalue.QValueKindTimestamp:
		return "TIMESTAMP_NTZ"
	case qvalue.QValueKindTimestampTZ:
		return "TIMESTAMP_TZ"
	case qvalue.QValueKindTime:
		return "TIME"
	case qvalue.QValueKindDate:
		return "DATE"
	// handle INTERVAL types again
	// case model.ColumnTypeTimeWithTimeZone, model.ColumnTypeInterval:
	// 	return "STRING"
	// bytes
	case qvalue.QValueKindBit, qvalue.QValueKindBytes:
		return "BINARY"
	// rest will be strings
	default:
		return "STRING"
	}
}

// snowflakeTypeToQValueKind converts a database type name to a QValueKind.
func snowflakeTypeToQValueKind(name string) (qvalue.QValueKind, error) {
	switch name {
	case "INT":
		return qvalue.QValueKindInt32, nil
	case "BIGINT":
		return qvalue.QValueKindInt64, nil
	case "FLOAT":
		return qvalue.QValueKindFloat32, nil
	case "DOUBLE", "REAL":
		return qvalue.QValueKindFloat64, nil
	case "VARCHAR", "CHAR", "TEXT":
		return qvalue.QValueKindString, nil
	case "BOOLEAN":
		return qvalue.QValueKindBoolean, nil
	// assuming TIMESTAMP is an alias to TIMESTAMP_NTZ, which is the default.
	case "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ":
		return qvalue.QValueKindTimestamp, nil
	case "TIMESTAMP_TZ":
		return qvalue.QValueKindTimestampTZ, nil
	case "TIME":
		return qvalue.QValueKindTime, nil
	case "DATE":
		return qvalue.QValueKindDate, nil
	case "BLOB", "BYTEA", "BINARY":
		return qvalue.QValueKindBytes, nil
	case "FIXED", "NUMBER", "DECIMAL", "NUMERIC":
		return qvalue.QValueKindNumeric, nil
	default:
		// If type is unsupported, return an error
		return "", fmt.Errorf("unsupported database type name: %s", name)
	}
}
