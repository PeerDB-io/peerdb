package datatypes

const (
	PeerDBBigQueryPrecision   = 38
	PeerDBBigQueryScale       = 9
	PeerDBSnowflakePrecision  = 38
	PeerDBSnowflakeScale      = 20
	PeerDBClickhousePrecision = 76
	PeerDBClickhouseScale     = 38
)

type WarehouseNumericComaptibility interface {
	MaxPrecision() int16
	MaxScale() int16
	DefaultPrecisionAndScale() (int16, int16)
	IsValidPrevisionAndScale(precision, scale int16) bool
}

type ClickHouseNumericCompatibility struct{}

func (c ClickHouseNumericCompatibility) MaxPrecision() int16 {
	return 76
}

func (c ClickHouseNumericCompatibility) MaxScale() int16 {
	return 38
}

func (c ClickHouseNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return PeerDBClickhousePrecision, PeerDBClickhouseScale
}

func (c ClickHouseNumericCompatibility) IsValidPrevisionAndScale(precision, scale int16) bool {
	return precision <= PeerDBClickhousePrecision && scale < precision
}

type SnowflakeNumericCompatibility struct{}

func (c SnowflakeNumericCompatibility) MaxPrecision() int16 {
	return 38
}

func (c SnowflakeNumericCompatibility) MaxScale() int16 {
	return 37
}

func (c SnowflakeNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return PeerDBSnowflakePrecision, PeerDBSnowflakeScale
}

func (c SnowflakeNumericCompatibility) IsValidPrevisionAndScale(precision, scale int16) bool {
	return precision <= 38 && scale < precision
}

type BigQueryNumericCompatibility struct{}

func (c BigQueryNumericCompatibility) MaxPrecision() int16 {
	return 38
}

func (c BigQueryNumericCompatibility) MaxScale() int16 {
	return 37
}

func (c BigQueryNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return PeerDBBigQueryPrecision, PeerDBBigQueryScale
}

func (c BigQueryNumericCompatibility) IsValidPrevisionAndScale(precision, scale int16) bool {
	return precision <= 38 && scale < precision
}

// This is to reverse what make_numeric_typmod of Postgres does:
// https://github.com/postgres/postgres/blob/21912e3c0262e2cfe64856e028799d6927862563/src/backend/utils/adt/numeric.c#L897
func ParseNumericTypmod(typmod int32) (int16, int16) {
	offsetMod := typmod - 4
	precision := int16((offsetMod >> 16) & 0x7FFF)
	scale := int16(offsetMod & 0x7FFF)
	return precision, scale
}
