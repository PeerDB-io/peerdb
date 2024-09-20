package qvalue

import (
	"time"

	"go.temporal.io/sdk/log"

	numeric "github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func DetermineNumericSettingForDWH(precision int16, scale int16, dwh protos.DBType) (int16, int16) {
	return numeric.GetNumericTypeForWarehouse(numeric.MakeNumericTypmod(int32(precision), int32(scale)), dwh)
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
