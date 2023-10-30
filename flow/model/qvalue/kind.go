package qvalue

import "fmt"

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

	// array types
	QValueKindArrayFloat32 QValueKind = "array_float32"
	QValueKindArrayFloat64 QValueKind = "array_float64"
	QValueKindArrayInt32   QValueKind = "array_int32"
	QValueKindArrayInt64   QValueKind = "array_int64"
	QValueKindArrayString  QValueKind = "array_string"
)

func QValueKindIsArray(kind QValueKind) bool {
	switch kind {
	case QValueKindArrayFloat32,
		QValueKindArrayFloat64,
		QValueKindArrayInt32,
		QValueKindArrayInt64,
		QValueKindArrayString:
		return true
	default:
		return false
	}
}

func (kind QValueKind) ToDWHColumnType(dwhType QDWHType) (string, error) {
	if dwhType != QDWHTypeSnowflake {
		return "", fmt.Errorf("unsupported DWH type: %v", dwhType)
	}

	switch kind {
	case QValueKindFloat32, QValueKindFloat64:
		return "FLOAT", nil
	case QValueKindInt16, QValueKindInt32, QValueKindInt64:
		return "INTEGER", nil
	case QValueKindBoolean:
		return "BOOLEAN", nil
	case QValueKindString:
		return "VARCHAR", nil
	case QValueKindTimestamp, QValueKindTimestampTZ:
		return "TIMESTAMP_NTZ", nil // or TIMESTAMP_TZ based on your needs
	case QValueKindDate:
		return "DATE", nil
	case QValueKindTime, QValueKindTimeTZ:
		return "TIME", nil
	case QValueKindNumeric:
		return "NUMBER", nil
	case QValueKindBytes:
		return "BINARY", nil
	case QValueKindUUID:
		return "VARCHAR", nil // Snowflake doesn't have a native UUID type
	case QValueKindJSON:
		return "VARIANT", nil
	case QValueKindArrayFloat32,
		QValueKindArrayFloat64,
		QValueKindArrayInt32,
		QValueKindArrayInt64,
		QValueKindArrayString:
		return "ARRAY", nil
	default:
		return "", fmt.Errorf("unsupported QValueKind: %v for dwhtype: %v", kind, dwhType)
	}
}
