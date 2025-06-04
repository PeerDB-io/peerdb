package clickhouse

// the type conversion represents a mapping from postgres -> clickhouse

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	ClickHouseMaxPrecision = 76
	ClickHouseMaxScale     = 38
)

var PostgresToClickHouseTypeMap = map[uint32]string{
	pgtype.BoolOID:             "Bool",
	pgtype.Int2OID:             "Int16",
	pgtype.Int4OID:             "Int32",
	pgtype.Int8OID:             "Int64",
	pgtype.Float4OID:           "Float32",
	pgtype.Float8OID:           "Float64",
	pgtype.QCharOID:            "FixedString(1)",
	pgtype.TextOID:             "String",
	pgtype.VarcharOID:          "String",
	pgtype.BPCharOID:           "String",
	pgtype.ByteaOID:            "String",
	pgtype.JSONOID:             "String",
	pgtype.JSONBOID:            "String",
	pgtype.UUIDOID:             "UUID",
	pgtype.TimeOID:             "DateTime64(6)",
	pgtype.DateOID:             "Date32",
	pgtype.CIDROID:             "String",
	pgtype.MacaddrOID:          "String",
	pgtype.InetOID:             "String",
	pgtype.TimestampOID:        "DateTime64(6)",
	pgtype.TimestamptzOID:      "DateTime64(6)",
	pgtype.TimetzOID:           "DateTime64(6)",
	pgtype.Int2ArrayOID:        "Array(Int16)",
	pgtype.Int4ArrayOID:        "Array(Int32)",
	pgtype.Int8ArrayOID:        "Array(Int64)",
	pgtype.PointOID:            "String",
	pgtype.Float4ArrayOID:      "Array(Float32)",
	pgtype.Float8ArrayOID:      "Array(Float64)",
	pgtype.BoolArrayOID:        "Array(Bool)",
	pgtype.DateArrayOID:        "Array(Date)",
	pgtype.TimestampArrayOID:   "Array(DateTime64(6))",
	pgtype.TimestamptzArrayOID: "Array(DateTime64(6))",
	pgtype.UUIDArrayOID:        "Array(UUID)",
	pgtype.TextArrayOID:        "Array(String)",
	pgtype.VarcharArrayOID:     "Array(String)",
	pgtype.BPCharArrayOID:      "Array(String)",
	pgtype.JSONArrayOID:        "String",
	pgtype.JSONBArrayOID:       "String",
	pgtype.IntervalOID:         "String",
	pgtype.TstzrangeOID:        "String",
}

var PostgresToSupportedClickHouseTypesMap = map[uint32][]string{
	pgtype.NumericOID: {"String"},
}

// TODO: handle nullable
// TODO: handle custom types
func PostgresToClickHouseType(oid uint32, typmod int32, nullable bool) string {
	var chType string

	if oid == pgtype.NumericOID {
		chType = getClickHouseTypeForNumericColumn(typmod)
	} else if t, ok := PostgresToClickHouseTypeMap[oid]; ok {
		chType = t
	} else {
		chType = "String"
	}

	return chType
}

func PostgresToSupportedClickHouseTypes(oid uint32, typmod int32, nullable bool) []string {
	chTypes := []string{PostgresToClickHouseType(oid, typmod, nullable)}
	if types, ok := PostgresToSupportedClickHouseTypesMap[oid]; ok {
		chTypes = append(chTypes, types...)
	}
	return chTypes
}

// copied and modified from flow/model/qvalue/kind.go
func getClickHouseTypeForNumericColumn(typmod int32) string {
	if typmod == -1 {
		return fmt.Sprintf("Decimal(%d, %d)", ClickHouseMaxPrecision, ClickHouseMaxScale)
	}

	precision, scale := shared.ParseNumericTypmod(typmod)
	if precision > ClickHouseMaxPrecision {
		return "String"
	}

	return fmt.Sprintf("Decimal(%d, %d)", precision, scale)
}
