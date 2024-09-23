package qvalue

import (
	"time"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

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
