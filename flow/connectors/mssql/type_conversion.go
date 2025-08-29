package connmssql

import (
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func QkindFromMssqlColumnType(ct string) (types.QValueKind, error) {
	switch strings.ToLower(ct) {
	case "bit":
		return types.QValueKindBoolean, nil
	case "tinyint":
		return types.QValueKindUInt8, nil
	case "smallint":
		return types.QValueKindInt16, nil
	case "int":
		return types.QValueKindInt32, nil
	case "bigint":
		return types.QValueKindInt64, nil
	case "real":
		return types.QValueKindFloat32, nil
	case "float":
		return types.QValueKindFloat64, nil
	case "decimal", "numeric", "money", "smallmoney":
		return types.QValueKindNumeric, nil
	case "char", "varchar", "text", "nchar", "nvarchar", "ntext", "sysname",
		"xml", "sql_variant", "hierarchyid":
		return types.QValueKindString, nil
	case "binary", "varbinary", "image", "timestamp":
		// timestamp (aka rowversion) is 8-byte binary
		return types.QValueKindBytes, nil
	case "date":
		return types.QValueKindDate, nil
	case "time":
		return types.QValueKindTime, nil
	case "datetime", "datetime2", "smalldatetime":
		return types.QValueKindTimestamp, nil
	case "datetimeoffset":
		return types.QValueKindTimestampTZ, nil
	case "uniqueidentifier":
		return types.QValueKindUUID, nil
	case "geometry":
		return types.QValueKindGeometry, nil
	case "geography":
		return types.QValueKindGeography, nil
	case "json":
		return types.QValueKindJSON, nil
	case "vector":
		return types.QValueKindArrayFloat32, nil
	default:
		// user-defined or unrecognized types fall back to string
		return types.QValueKindString, nil
	}
}
