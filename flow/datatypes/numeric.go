package datatypes

const (
	// defaults
	PeerDBBigQueryScale   = 20
	PeerDBSnowflakeScale  = 20
	PeerDBClickHouseScale = 38

	PeerDBClickHouseMaxPrecision = 76
	VARHDRSZ                     = 4
)

type WarehouseNumericCompatibility interface {
	MaxPrecision() int16
	MaxScale() int16
	DefaultPrecisionAndScale() (int16, int16)
}

type ClickHouseNumericCompatibility struct{}

func (ClickHouseNumericCompatibility) MaxPrecision() int16 {
	return PeerDBClickHouseMaxPrecision
}

func (ClickHouseNumericCompatibility) MaxScale() int16 {
	return 38
}

func (c ClickHouseNumericCompatibility) DefaultPrecisionAndScale() (int16, int16) {
	return c.MaxPrecision(), PeerDBClickHouseScale
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

func IsValidPrecision(precision int16, warehouseNumeric WarehouseNumericCompatibility) bool {
	return precision <= warehouseNumeric.MaxPrecision()
}

func IsValidPrecisionAndScale(precision, scale int16, warehouseNumeric WarehouseNumericCompatibility) bool {
	return IsValidPrecision(precision, warehouseNumeric) && scale <= warehouseNumeric.MaxScale()
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
	if typmod == -1 {
		return 0, 0
	}

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
	return GetNumericTypeForWarehousePrecisionScale(precision, scale, warehouseNumeric)
}

func GetNumericTypeForWarehousePrecisionScale(precision int16, scale int16, warehouseNumeric WarehouseNumericCompatibility) (int16, int16) {
	if precision == 0 && scale == 0 {
		return warehouseNumeric.DefaultPrecisionAndScale()
	}

	if !IsValidPrecision(precision, warehouseNumeric) {
		precision = warehouseNumeric.MaxPrecision()
	}

	if !IsValidPrecisionAndScale(precision, scale, warehouseNumeric) {
		precision, scale = warehouseNumeric.DefaultPrecisionAndScale()
	}

	return precision, scale
}
