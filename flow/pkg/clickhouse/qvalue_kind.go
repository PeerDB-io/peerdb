package clickhouse

import (
	"fmt"
	"strings"
)

// QValueKindForType maps a ClickHouse column type to the QValueKind expected for its values,
// returned as the string form of the flow module's types.QValueKind.
func QValueKindForType(columnType string) (string, error) {
	switch columnType {
	case "String", "Nullable(String)", "LowCardinality(String)", "LowCardinality(Nullable(String))":
		return "string", nil
	case "Bool", "Nullable(Bool)":
		return "bool", nil
	case "Int8", "Nullable(Int8)":
		return "int8", nil
	case "Int16", "Nullable(Int16)":
		return "int16", nil
	case "Int32", "Nullable(Int32)":
		return "int32", nil
	case "Int64", "Nullable(Int64)":
		return "int64", nil
	case "Int256", "Nullable(Int256)":
		return "int256", nil
	case "UInt8", "Nullable(UInt8)":
		return "uint8", nil
	case "UInt16", "Nullable(UInt16)":
		return "uint16", nil
	case "UInt32", "Nullable(UInt32)":
		return "uint32", nil
	case "UInt64", "Nullable(UInt64)":
		return "uint64", nil
	case "UInt256", "Nullable(UInt256)":
		return "uint256", nil
	case "UUID", "Nullable(UUID)":
		return "uuid", nil
	case "DateTime64(6)", "Nullable(DateTime64(6))", "DateTime64(9)", "Nullable(DateTime64(9))":
		return "timestamp", nil
	case "Time64(6)", "Nullable(Time64(6))":
		return "time", nil
	case "Date32", "Nullable(Date32)":
		return "date", nil
	case "Float32", "Nullable(Float32)":
		return "float32", nil
	case "Float64", "Nullable(Float64)":
		return "float64", nil
	case "Array(Int32)":
		return "array_int32", nil
	case "Array(Float32)":
		return "array_float32", nil
	case "Array(Float64)":
		return "array_float64", nil
	case "Array(String)", "Array(LowCardinality(String))":
		return "array_string", nil
	case "Array(UUID)":
		return "array_uuid", nil
	case "Array(DateTime64(6))":
		return "array_timestamp", nil
	case "Array(Int64)":
		return "array_int64", nil
	case "Array(Bool)":
		return "array_bool", nil
	case "Array(Date)":
		return "array_date", nil
	case "JSON", "Nullable(JSON)":
		return "json", nil
	default:
		if strings.Contains(columnType, "Decimal") {
			if strings.HasPrefix(columnType, "Array(") {
				return "array_numeric", nil
			}
			return "numeric", nil
		}
		return "", fmt.Errorf("failed to resolve QValueKind for %s", columnType)
	}
}
