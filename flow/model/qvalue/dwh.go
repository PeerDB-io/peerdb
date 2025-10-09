package qvalue

import (
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
)

func DetermineNumericSettingForDWH(precision int16, scale int16, dwh protos.DBType) (int16, int16) {
	var warehouseNumeric datatypes.WarehouseNumericCompatibility
	switch dwh {
	case protos.DBType_CLICKHOUSE:
		warehouseNumeric = datatypes.ClickHouseNumericCompatibility{}
	case protos.DBType_SNOWFLAKE:
		warehouseNumeric = datatypes.SnowflakeNumericCompatibility{}
	case protos.DBType_BIGQUERY:
		warehouseNumeric = datatypes.BigQueryNumericCompatibility{}
	default:
		warehouseNumeric = datatypes.DefaultNumericCompatibility{}
	}

	return datatypes.GetNumericTypeForWarehousePrecisionScale(precision, scale, warehouseNumeric)
}

func DefaultTime(dwh protos.DBType) time.Time {
	if dwh == protos.DBType_CLICKHOUSE {
		// ClickHouse coerces NULL to Unix epoch, which is valid for all their time types,
		// even when Date32 & DateTime64 can represent lower times.
		return time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
	return time.Time{}
}
