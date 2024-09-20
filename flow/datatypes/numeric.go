package datatypes

import "github.com/PeerDB-io/peer-flow/generated/protos"

const (
	VARHDRSZ = 4

	// default scale
	bigQueryDefaultScale   = 20
	snowflakeDefaultScale  = 20
	clickHouseDefaultScale = 38
	genericDefaultScale    = 20

	// max scale
	bigQueryMaxScale   = 38
	snowflakeMaxScale  = 37
	clickHouseMaxScale = 38
	genericMaxScale    = 37

	// default/max precision
	bigQueryPrecision   = 76
	snowflakePrecision  = 38
	clickHousePrecision = 76
	genericPrecision    = 38
)

var defaultScaleMap = map[protos.DBType]int16{
	protos.DBType_BIGQUERY:   bigQueryDefaultScale,
	protos.DBType_SNOWFLAKE:  snowflakeDefaultScale,
	protos.DBType_CLICKHOUSE: clickHouseDefaultScale,
}

var maxScaleMap = map[protos.DBType]int16{
	protos.DBType_BIGQUERY:   bigQueryMaxScale,
	protos.DBType_SNOWFLAKE:  snowflakeMaxScale,
	protos.DBType_CLICKHOUSE: clickHouseMaxScale,
}

var precisionMap = map[protos.DBType]int16{
	protos.DBType_BIGQUERY:   bigQueryPrecision,
	protos.DBType_SNOWFLAKE:  snowflakePrecision,
	protos.DBType_CLICKHOUSE: clickHousePrecision,
}

func getMaxPrecisionForDWH(dwh protos.DBType) int16 {
	precision, ok := precisionMap[dwh]
	if !ok {
		return genericPrecision
	}
	return precision
}

func getMaxScaleForDWH(dwh protos.DBType) int16 {
	scale, ok := maxScaleMap[dwh]
	if !ok {
		return genericMaxScale
	}
	return scale
}

func getDefaultPrecisionAndScaleForDWH(dwh protos.DBType) (int16, int16) {
	defaultScale, ok := defaultScaleMap[dwh]
	if !ok {
		return getMaxPrecisionForDWH(dwh), genericDefaultScale
	}
	return getMaxPrecisionForDWH(dwh), defaultScale
}

func isValidPrecision(precision int16, dwh protos.DBType) bool {
	return precision > 0 && precision <= getMaxPrecisionForDWH(dwh)
}

func isValidScale(precision int16, scale int16, dwh protos.DBType) bool {
	return scale > 0 &&
		isValidPrecision(precision, dwh) &&
		scale <= getMaxScaleForDWH(dwh) &&
		scale < precision
}

func MakeNumericTypmod(precision int32, scale int32) int32 {
	if precision == 0 && scale == 0 {
		return -1
	}
	return (precision << 16) | (scale & 0x7ff) + VARHDRSZ
}

// This is to reverse what make_numeric_typmod of Postgres does:
// https://github.com/postgres/postgres/blob/21912e3c0262e2cfe64856e028799d6927862563/src/backend/utils/adt/numeric.c#L897
func ParseNumericTypmod(typmod int32) (int16, int16) {
	offsetMod := typmod - VARHDRSZ
	precision := int16((offsetMod >> 16) & 0x7FFF)
	scale := int16(offsetMod & 0x7FFF)
	return precision, scale
}

func GetNumericTypeForWarehouse(typmod int32, dwh protos.DBType) (int16, int16) {
	if typmod == -1 {
		return getDefaultPrecisionAndScaleForDWH(dwh)
	}

	precision, scale := ParseNumericTypmod(typmod)
	if !isValidPrecision(precision, dwh) {
		precision = getMaxPrecisionForDWH(dwh)
	}

	if !isValidScale(precision, scale, dwh) {
		scale = getMaxScaleForDWH(dwh)
	}

	return precision, scale
}
