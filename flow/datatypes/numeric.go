package datatypes

import "github.com/PeerDB-io/peerdb/flow/shared"

const (
	// defaults
	PeerDBBigQueryScale   = 20
	PeerDBSnowflakeScale  = 20
	PeerDBClickHouseScale = 38

	PeerDBClickHouseMaxPrecision = 76
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
	return (precision << 16) | (scale & 0x7ff) + shared.VARHDRSZ
}

func GetNumericTypeForWarehouse(typmod int32, warehouseNumeric WarehouseNumericCompatibility) (int16, int16) {
	if typmod == -1 {
		return warehouseNumeric.DefaultPrecisionAndScale()
	}

	precision, scale := shared.ParseNumericTypmod(typmod)
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
