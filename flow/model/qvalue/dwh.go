package qvalue

import (
	"time"

	"go.temporal.io/sdk/log"

	numeric "github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func DetermineNumericSettingForDWH(precision int16, scale int16, dwh protos.DBType) (int16, int16) {
	if dwh == protos.DBType_CLICKHOUSE {
		if precision > numeric.PeerDBClickhousePrecision || precision <= 0 ||
			scale > precision || scale < 0 {
			return numeric.PeerDBClickhousePrecision, numeric.PeerDBClickhouseScale
		}
	} else {
		if precision > numeric.PeerDBNumericPrecision || precision <= 0 ||
			scale > numeric.PeerDBNumericScale || scale < 0 {
			return numeric.PeerDBNumericPrecision, numeric.PeerDBNumericScale
		}
	}

	return precision, scale
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
