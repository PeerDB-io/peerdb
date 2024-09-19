package datatypes

const (
	// defaults
	PeerDBBigQueryScale   = 20
	PeerDBSnowflakeScale  = 20
	PeerDBClickhouseScale = 38
	VARHDRSZ              = 4
)

type WarehouseNumericCompatibility interface {
	MaxPrecision() int16
	MaxScale() int16
	DefaultPrecisionAndScale() (int16, int16)
	IsValidPrecisionAndScale(precision, scale int16) bool
	IsValidPrecision(precision int16) bool
}

type ClickHouseNumericCompatibility struct{}

func (ClickHouseNumericCompatibility) MaxPrecision() int16 {
	return 76
}

func (ClickHouseNumericCompatibility) MaxScale() int16 {
	return 38
}

func (c ClickHouseNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return c.MaxPrecision(), PeerDBClickhouseScale
}

func (c ClickHouseNumericCompatibility) IsValidPrecisionAndScale(precision, scale int16) bool {
	return c.IsValidPrecision(precision) && scale >= 0 && scale < precision
}

func (c ClickHouseNumericCompatibility) IsValidPrecision(precision int16) bool {
	return precision > 0 && precision <= c.MaxPrecision()
}

type SnowflakeNumericCompatibility struct{}

func (SnowflakeNumericCompatibility) MaxPrecision() int16 {
	return 38
}

func (SnowflakeNumericCompatibility) MaxScale() int16 {
	return 37
}

func (s SnowflakeNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return s.MaxPrecision(), PeerDBSnowflakeScale
}

func (s SnowflakeNumericCompatibility) IsValidPrecisionAndScale(precision, scale int16) bool {
	return s.IsValidPrecision(precision) && scale >= 0 && scale < precision
}

func (s SnowflakeNumericCompatibility) IsValidPrecision(precision int16) bool {
	return precision > 0 && precision <= s.MaxPrecision()
}

type BigQueryNumericCompatibility struct{}

func (BigQueryNumericCompatibility) MaxPrecision() int16 {
	return 38
}

func (BigQueryNumericCompatibility) MaxScale() int16 {
	return 20
}

func (b BigQueryNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return b.MaxPrecision(), PeerDBBigQueryScale
}

func (b BigQueryNumericCompatibility) IsValidPrecisionAndScale(precision, scale int16) bool {
	return b.IsValidPrecision(precision) && scale >= 0 && scale < precision
}

func (b BigQueryNumericCompatibility) IsValidPrecision(precision int16) bool {
	return precision > 0 && precision <= b.MaxPrecision()
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

func (d DefaultNumericCompatibility) IsValidPrecisionAndScale(precision, scale int16) bool {
	return d.IsValidPrecision(precision) && scale >= 0 && scale < precision
}

func (d DefaultNumericCompatibility) IsValidPrecision(precision int16) bool {
	return precision > 0 && precision <= d.MaxPrecision()
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
	if !warehouseNumeric.IsValidPrecision(precision) {
		precision = warehouseNumeric.MaxPrecision()
	}

	if !warehouseNumeric.IsValidPrecisionAndScale(precision, scale) {
		precision, scale = warehouseNumeric.DefaultPrecisionAndScale()
	}

	return precision, scale
}
