package connmysql

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func QkindFromMysqlColumnType(ct string, binlogRowMetadataSupported bool, version uint32) (types.QValueKind, error) {
	// https://mariadb.com/docs/server/reference/data-types/date-and-time-data-types/timestamp#tab-current-1
	ct, _ = strings.CutSuffix(ct, " /* mariadb-5.3 */")
	ct, _ = strings.CutSuffix(ct, " zerofill")
	ct, isUnsigned := strings.CutSuffix(ct, " unsigned")
	ct, param, _ := strings.Cut(ct, "(")
	switch strings.ToLower(ct) {
	case "json":
		return types.QValueKindJSON, nil
	case "char", "varchar", "text", "set", "tinytext", "mediumtext", "longtext":
		return types.QValueKindString, nil
	case "enum":
		if !binlogRowMetadataSupported && version >= shared.InternalVersion_MySQL5ConvertEnumsToInts {
			return types.QValueKindUint16Enum, nil
		}
		return types.QValueKindEnum, nil
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return types.QValueKindBytes, nil
	case "date":
		return types.QValueKindDate, nil
	case "datetime", "timestamp":
		return types.QValueKindTimestamp, nil
	case "time":
		return types.QValueKindTime, nil
	case "decimal", "numeric":
		return types.QValueKindNumeric, nil
	case "float":
		return types.QValueKindFloat32, nil
	case "double":
		return types.QValueKindFloat64, nil
	case "tinyint":
		if strings.HasPrefix(param, "1)") {
			return types.QValueKindBoolean, nil
		} else if isUnsigned {
			return types.QValueKindUInt8, nil
		} else {
			return types.QValueKindInt8, nil
		}
	case "smallint", "year":
		if isUnsigned {
			return types.QValueKindUInt16, nil
		} else {
			return types.QValueKindInt16, nil
		}
	case "mediumint", "int":
		if isUnsigned {
			return types.QValueKindUInt32, nil
		} else {
			return types.QValueKindInt32, nil
		}
	case "bit":
		return types.QValueKindUInt64, nil
	case "bigint":
		if isUnsigned {
			return types.QValueKindUInt64, nil
		} else {
			return types.QValueKindInt64, nil
		}
	case "vector":
		return types.QValueKindArrayFloat32, nil
	case "uuid": // MariaDB 10.7+
		return types.QValueKindUUID, nil
	case "inet4", "inet6": // MariaDB 10.10+ / 10.5+; both rendered as text
		return types.QValueKindINET, nil
	case "geometry", "point", "polygon", "linestring", "multipoint", "multilinestring", "multipolygon", "geomcollection", "geometrycollection":
		return types.QValueKindGeometry, nil
	default:
		return types.QValueKind(""), fmt.Errorf("unknown mysql type %s", ct)
	}
}

// isBinlogStringBackedType reports whether a QValueKind comes from a MariaDB type that
// is transmitted in the binlog as MYSQL_TYPE_STRING with the binary charset even though
// its information_schema data type is something more specific. Verified against MariaDB
// 11.8: uuid/inet4/inet6 all arrive with ColumnType 0xFE (MYSQL_TYPE_STRING) and binary
// charset — byte-for-byte indistinguishable from BINARY(N) — so qkindFromMysqlType maps
// them to bytes. That wire-derived qkind legitimately differs from the schema's qkind
// (inet/uuid), so the mismatch must not be reported as a column type change.
func isBinlogStringBackedType(kind types.QValueKind) bool {
	switch kind {
	case types.QValueKindUUID, types.QValueKindINET:
		return true
	default:
		return false
	}
}
