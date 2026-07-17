package datatypes

import (
	pkgdatatypes "github.com/PeerDB-io/peerdb/flow/pkg/datatypes"
)

// The numeric warehouse-compatibility helpers live in flow/pkg/datatypes so
// that code in the flow/pkg module can use them; the aliases below keep
// existing importers of this package working.
const (
	PeerDBBigQueryScale   = pkgdatatypes.PeerDBBigQueryScale
	PeerDBSnowflakeScale  = pkgdatatypes.PeerDBSnowflakeScale
	PeerDBClickHouseScale = pkgdatatypes.PeerDBClickHouseScale

	PeerDBClickHouseMaxPrecision = pkgdatatypes.PeerDBClickHouseMaxPrecision
	VARHDRSZ                     = pkgdatatypes.VARHDRSZ
)

type (
	WarehouseNumericCompatibility  = pkgdatatypes.WarehouseNumericCompatibility
	ClickHouseNumericCompatibility = pkgdatatypes.ClickHouseNumericCompatibility
	SnowflakeNumericCompatibility  = pkgdatatypes.SnowflakeNumericCompatibility
	BigQueryNumericCompatibility   = pkgdatatypes.BigQueryNumericCompatibility
	DefaultNumericCompatibility    = pkgdatatypes.DefaultNumericCompatibility
)

var (
	IsValidPrecision                         = pkgdatatypes.IsValidPrecision
	IsValidPrecisionAndScale                 = pkgdatatypes.IsValidPrecisionAndScale
	MakeNumericTypmod                        = pkgdatatypes.MakeNumericTypmod
	GetNumericTypeForWarehouse               = pkgdatatypes.GetNumericTypeForWarehouse
	GetNumericTypeForWarehousePrecisionScale = pkgdatatypes.GetNumericTypeForWarehousePrecisionScale
)
