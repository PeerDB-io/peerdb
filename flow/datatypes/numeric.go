package datatypes

const (
	// defaults
	PeerDBBigQueryPrecision   = 38
	PeerDBBigQueryScale       = 20
	PeerDBSnowflakePrecision  = 38
	PeerDBSnowflakeScale      = 20
	PeerDBClickhousePrecision = 76
	PeerDBClickhouseScale     = 38
)

type WarehouseNumericCompatibility interface {
	MaxPrecision() int16
	MaxScale() int16
	DefaultPrecisionAndScale() (int16, int16)
	IsValidPrevisionAndScale(precision, scale int16) bool
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

func (ClickHouseNumericCompatibility) IsValidPrevisionAndScale(precision, scale int16) bool {
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

func (SnowflakeNumericCompatibility) IsValidPrevisionAndScale(precision, scale int16) bool {
	return precision > 0 && precision <= 38 && scale < precision
}

type BigQueryNumericCompatibility struct{}

func (BigQueryNumericCompatibility) MaxPrecision() int16 {
	return 38
}

func (BigQueryNumericCompatibility) MaxScale() int16 {
	return 37
}

func (BigQueryNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return PeerDBBigQueryPrecision, PeerDBBigQueryScale
}

func (BigQueryNumericCompatibility) IsValidPrevisionAndScale(precision, scale int16) bool {
	return precision > 0 && precision <= 38 && scale < precision
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

func (DefaultNumericCompatibility) IsValidPrevisionAndScale(precision, scale int16) bool {
	return true
}

// This is to reverse what make_numeric_typmod of Postgres does:
// https://github.com/postgres/postgres/blob/21912e3c0262e2cfe64856e028799d6927862563/src/backend/utils/adt/numeric.c#L897
func ParseNumericTypmod(typmod int32) (int16, int16) {
	offsetMod := typmod - 4
	precision := int16((offsetMod >> 16) & 0x7FFF)
	scale := int16(offsetMod & 0x7FFF)
	return precision, scale
}

func GetNumericTypeForWarehouse(typmod int32, warehouseNumeric WarehouseNumericCompatibility) (int16, int16) {
	if typmod == -1 {
		return warehouseNumeric.DefaultPrecisionAndScale()
	}

	precision, scale := ParseNumericTypmod(typmod)
	if !warehouseNumeric.IsValidPrevisionAndScale(precision, scale) {
		return warehouseNumeric.DefaultPrecisionAndScale()
	}

	return precision, scale
}
