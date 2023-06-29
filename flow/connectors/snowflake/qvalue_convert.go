package connsnowflake

import (
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func getSnowflakeTypeForGenericColumnType(colType string) string {
	switch qvalue.QValueKind(colType) {
	case qvalue.QValueKindBoolean:
		return "BOOLEAN"
	// integer types
	case qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64:
		return "INTEGER"
	// decimal types
	// The names FLOAT, FLOAT4, and FLOAT8 are for compatibility with other systems
	// Snowflake treats all three as 64-bit floating-point numbers.
	case qvalue.QValueKindFloat16, qvalue.QValueKindFloat32, qvalue.QValueKindFloat64:
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
	// TODO: handle other time types, since ETime isn't being granular enough here.
	case qvalue.QValueKindETime:
		return "TIMESTAMP_NTZ"
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
