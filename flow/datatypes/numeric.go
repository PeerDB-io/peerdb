package datatypes

const (
	// defaults
	PeerDBBigQueryPrecision   = 38
	PeerDBBigQueryScale       = 20
	PeerDBSnowflakePrecision  = 38
	PeerDBSnowflakeScale      = 20
	PeerDBClickhousePrecision = 76
	PeerDBClickhouseScale     = 38
	VARHDRSZ                  = 4
)

type WarehouseNumericCompatibility interface {
	MaxPrecision() int16
	MaxScale() int16
	DefaultPrecisionAndScale() (int16, int16)
	IsValidPrecisionAndScale(precision, scale int16) bool
}

type ClickHouseNumericCompatibility struct{}

func (ClickHouseNumericCompatibility) MaxPrecision() int16 {
	return 76
}

func (ClickHouseNumericCompatibility) MaxScale() int16 {
	return 38
}

func (ClickHouseNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return PeerDBClickhousePrecision, PeerDBClickhouseScale
}

func (ClickHouseNumericCompatibility) IsValidPrecisionAndScale(precision, scale int16) bool {
	return precision > 0 && precision <= PeerDBClickhousePrecision && scale < precision
}

type SnowflakeNumericCompatibility struct{}

func (SnowflakeNumericCompatibility) MaxPrecision() int16 {
	return 38
}

func (SnowflakeNumericCompatibility) MaxScale() int16 {
	return 37
}

func (SnowflakeNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return PeerDBSnowflakePrecision, PeerDBSnowflakeScale
}

func (SnowflakeNumericCompatibility) IsValidPrecisionAndScale(precision, scale int16) bool {
	return precision > 0 && precision <= 38 && scale < precision
}

type BigQueryNumericCompatibility struct{}

func (BigQueryNumericCompatibility) MaxPrecision() int16 {
	return 38
}

func (BigQueryNumericCompatibility) MaxScale() int16 {
	return 20
}

func (BigQueryNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return PeerDBBigQueryPrecision, PeerDBBigQueryScale
}

func (BigQueryNumericCompatibility) IsValidPrecisionAndScale(precision, scale int16) bool {
	return precision > 0 && precision <= PeerDBBigQueryPrecision &&
		scale <= PeerDBBigQueryScale && scale < precision
}

type DefaultNumericCompatibility struct{}

func (DefaultNumericCompatibility) MaxPrecision() int16 {
	return 38
}

func (DefaultNumericCompatibility) MaxScale() int16 {
	return 37
}

func (DefaultNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return 38, 20
}

func (DefaultNumericCompatibility) IsValidPrecisionAndScale(precision, scale int16) bool {
	return true
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

func GetNumericTypeForWarehouse(typmod int32, warehouseNumeric WarehouseNumericCompatibility) (int16, int16) {
	if typmod == -1 {
		return warehouseNumeric.DefaultPrecisionAndScale()
	}

	precision, scale := ParseNumericTypmod(typmod)
	if !warehouseNumeric.IsValidPrecisionAndScale(precision, scale) {
		return warehouseNumeric.DefaultPrecisionAndScale()
	}

	return precision, scale
}
