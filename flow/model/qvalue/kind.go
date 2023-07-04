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
	QValueKindArray       QValueKind = "array"
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
)
