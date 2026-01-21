package connmysql

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func QkindFromMysqlColumnType(ct string) (types.QValueKind, error) {
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
	case "geometry", "point", "polygon", "linestring", "multipoint", "multipolygon", "geomcollection":
		return types.QValueKindGeometry, nil
	default:
		return types.QValueKind(""), fmt.Errorf("unknown mysql type %s", ct)
	}
}
