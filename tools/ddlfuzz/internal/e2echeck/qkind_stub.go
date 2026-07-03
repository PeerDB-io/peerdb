//go:build !ddlfuzz

package e2echeck

import "strings"

func qkindString(typeStr string) string {
	ct, _ := strings.CutSuffix(typeStr, " /* mariadb-5.3 */")
	ct, _ = strings.CutSuffix(ct, " zerofill")
	ct, isUnsigned := strings.CutSuffix(ct, " unsigned")
	ct, param, _ := strings.Cut(ct, "(")
	switch strings.ToLower(ct) {
	case "json":
		return "json"
	case "char", "varchar", "text", "set", "tinytext", "mediumtext", "longtext":
		return "string"
	case "enum":
		return "enum"
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return "bytes"
	case "date":
		return "date"
	case "datetime", "timestamp":
		return "timestamp"
	case "time":
		return "time"
	case "decimal", "numeric":
		return "numeric"
	case "float":
		return "float32"
	case "double":
		return "float64"
	case "tinyint":
		if strings.HasPrefix(param, "1)") {
			return "bool"
		}
		if isUnsigned {
			return "uint8"
		}
		return "int8"
	case "smallint", "year":
		if isUnsigned {
			return "uint16"
		}
		return "int16"
	case "mediumint", "int":
		if isUnsigned {
			return "uint32"
		}
		return "int32"
	case "bit":
		return "uint64"
	case "bigint":
		if isUnsigned {
			return "uint64"
		}
		return "int64"
	case "vector":
		return "array_float32"
	case "geometry", "point", "polygon", "linestring", "multipoint", "multilinestring", "multipolygon", "geomcollection", "geometrycollection":
		return "geometry"
	default:
		return "ERR"
	}
}
