package qvalue

import (
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
