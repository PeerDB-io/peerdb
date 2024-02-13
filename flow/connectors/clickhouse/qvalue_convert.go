package connclickhouse

import (
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

// TODO: remove extra types from here
var clickhouseTypeToQValueKindMap = map[string]qvalue.QValueKind{
	"INT":            qvalue.QValueKindInt32,
	"Int64":          qvalue.QValueKindInt64,
	"Int16":          qvalue.QValueKindInt16,
	"Float64":        qvalue.QValueKindFloat64,
	"DOUBLE":         qvalue.QValueKindFloat64,
	"REAL":           qvalue.QValueKindFloat64,
	"VARCHAR":        qvalue.QValueKindString,
	"CHAR":           qvalue.QValueKindString,
	"TEXT":           qvalue.QValueKindString,
	"String":         qvalue.QValueKindString,
	"Bool":           qvalue.QValueKindBoolean,
	"DateTime":       qvalue.QValueKindTimestamp,
	"TIMESTAMP":      qvalue.QValueKindTimestamp,
	"DateTime64(6)":  qvalue.QValueKindTimestamp,
	"TIMESTAMP_NTZ":  qvalue.QValueKindTimestamp,
	"TIMESTAMP_TZ":   qvalue.QValueKindTimestampTZ,
	"TIME":           qvalue.QValueKindTime,
	"UUID":           qvalue.QValueKindUUID,
	"DATE":           qvalue.QValueKindDate,
	"BLOB":           qvalue.QValueKindBytes,
	"BYTEA":          qvalue.QValueKindBytes,
	"BINARY":         qvalue.QValueKindBytes,
	"FIXED":          qvalue.QValueKindNumeric,
	"NUMBER":         qvalue.QValueKindNumeric,
	"DECIMAL":        qvalue.QValueKindNumeric,
	"NUMERIC":        qvalue.QValueKindNumeric,
	"VARIANT":        qvalue.QValueKindJSON,
	"GEOMETRY":       qvalue.QValueKindGeometry,
	"GEOGRAPHY":      qvalue.QValueKindGeography,
	"Array(String)":  qvalue.QValueKindArrayString,
	"Array(Int32)":   qvalue.QValueKindArrayInt32,
	"Array(Int64)":   qvalue.QValueKindArrayInt64,
	"Array(Float64)": qvalue.QValueKindArrayFloat64,
}

func qValueKindToClickhouseType(colType qvalue.QValueKind) (string, error) {
	val, err := colType.ToDWHColumnType(qvalue.QDWHTypeClickhouse)
	if err != nil {
		return "", err
	}

	return val, err
}
