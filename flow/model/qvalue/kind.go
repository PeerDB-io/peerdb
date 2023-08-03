package qvalue

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
