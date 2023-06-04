package model

// ColumnType is an enum for the column type, which are generic across all connectors.
const (
	ColumnTypeInt16                 = "int16"
	ColumnTypeInt32                 = "int32"
	ColumnTypeInt64                 = "int64"
	ColumnHexBytesString            = "bytes"
	ColumnTypeBoolean               = "bool"
	ColumnTypeFloat16               = "float16"
	ColumnTypeFloat32               = "float32"
	ColumnTypeFloat64               = "float64"
	ColumnTypeString                = "string"
	ColumnTypeNumeric               = "numeric"
	ColumnTypeJSON                  = "json"
	ColumnTypeInterval              = "interval"
	ColumnTypeTimestamp             = "timestamp"
	ColumnTypeTimeStampWithTimeZone = "timestamptz"
	ColumnTypeTime                  = "time"
	ColumnTypeTimeWithTimeZone      = "timetz"
	ColumnTypeDate                  = "date"
)
