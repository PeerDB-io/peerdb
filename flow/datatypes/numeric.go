package datatypes

import (
	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

const (
	VARHDRSZ = 4

	// default scale
	bigQueryDefaultScale   = bigquery.BigNumericScaleDigits
	snowflakeDefaultScale  = 20
	clickHouseDefaultScale = 38
	genericDefaultScale    = 20

	// max scale
	bigQueryMaxScale   = bigquery.BigNumericScaleDigits
	snowflakeMaxScale  = 37
	clickHouseMaxScale = 38
	genericMaxScale    = 37

	// default/max precision
	bigQueryPrecision   = bigquery.BigNumericPrecisionDigits
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
	return scale >= 0 &&
		isValidPrecision(precision, dwh) &&
		scale <= getMaxScaleForDWH(dwh)
}

/*
As far as my understanding from Postgres source code goes:
 1. typmod itself is a 32-bit integer.
 2. In the case of NUMERICs, it will be -1 or > VARHDRSZ.
 3. If it is -1, it means that the precision and scale are not specified and it is an "unconstrained" NUMERIC.
 4. If it is > VARHDRSZ, it means that the precision is specified and scale MAY be specified. Otherwise, scale defaults to 0.
 5. This is a "constrained" NUMERIC. Precision in this case ranges only from 1 to 1000, far less than the unconstrained limit.
 6. The scale in this case ranges from -1000 to 1000. Yes, it can be negative. Yes, it can be more than the precision.

Currently, no DWH supports the two weird cases of scales in Postgres NUMERICs. Expected is that the 0 <= scale < precision.
In this case, we will default to the default scale and maximum precision for the DWH.
*/
type NumericTypmod struct {
	constrained bool
	precision   int16
	scale       int16
}

// This is to reverse what make_numeric_typmod of Postgres does:
// logic copied from: https://github.com/postgres/postgres/blob/c4d5cb71d229095a39fda1121a75ee40e6069a2a/src/backend/utils/adt/numeric.c#L929
// Maps most "invalid" typmods to be unconstrained (same as -1)
func NewParsedNumericTypmod(typmod int32) *NumericTypmod {
	if typmod < VARHDRSZ {
		return &NumericTypmod{
			constrained: false,
		}
	}

	typmod -= VARHDRSZ
	// if precision or scale are out of bounds, switch to unconstrained and hope for the best
	precision := int16((typmod >> 16) & 0xFFFF)
	scale := int16(((typmod & 0x7ff) ^ 1024) - 1024)
	if precision < 1 || precision > 1000 || scale < -1000 || scale > 1000 {
		return &NumericTypmod{
			constrained: false,
		}
	}
	return &NumericTypmod{
		constrained: true,
		precision:   int16((typmod >> 16) & 0xFFFF),
		scale:       int16(((typmod & 0x7ff) ^ 1024) - 1024),
	}
}

// responsibility of caller to ensure sensible values are passed in
func NewConstrainedNumericTypmod(precision int16, scale int16) *NumericTypmod {
	return &NumericTypmod{
		constrained: true,
		precision:   precision,
		scale:       scale,
	}
}

func (t *NumericTypmod) ToTypmod() int32 {
	if t == nil || !t.constrained {
		return -1
	}
	return ((int32(t.precision) << 16) | (int32(t.scale) & 0x7ff)) + VARHDRSZ
}

func (t *NumericTypmod) PrecisionAndScale() (int16, int16) {
	if t == nil {
		return 0, 0
	}
	return t.precision, t.scale
}

func (t *NumericTypmod) ToDWHNumericConstraints(dwh protos.DBType) (int16, int16) {
	if t == nil || !t.constrained {
		return getDefaultPrecisionAndScaleForDWH(dwh)
	}

	precision, scale := t.precision, t.scale
	if !isValidPrecision(t.precision, dwh) {
		precision = getMaxPrecisionForDWH(dwh)
	}
	if !isValidScale(t.precision, t.scale, dwh) {
		precision, scale = getDefaultPrecisionAndScaleForDWH(dwh)
	}

	return precision, scale
}
