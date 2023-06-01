package model

// ColumnType is an enum for the column type, which are generic across all connectors.
const (
	ColumnTypeInt16     = "int16"
	ColumnTypeInt32     = "int32"
	ColumnTypeInt64     = "int64"
	ColumnTypeBytea     = "bytes"
	ColumnTypeBoolean   = "bool"
	ColumnTypeTimestamp = "timestamp"
	ColumnTypeFloat16   = "float16"
	ColumnTypeFloat32   = "float32"
	ColumnTypeFloat64   = "float64"
	ColumnTypeString    = "string"
	ColumnTypeUuid      = "uuid"
	ColumnTypeNumeric   = "numeric"
	ColumnTypeJSON      = "json"
)
