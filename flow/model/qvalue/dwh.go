package qvalue

import (
	"time"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
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

// Bigquery will not allow timestamp if it is less than 1AD and more than 9999AD
func DisallowedTimestamp(dwh protos.DBType, t time.Time, logger log.Logger) bool {
	if dwh == protos.DBType_BIGQUERY {
		year := t.Year()
		if year < 1 || year > 9999 {
			logger.Warn("Nulling Timestamp value for BigQuery as it exceeds allowed range",
				"timestamp", t.String())
			return true
		}
	}
	return false
}
