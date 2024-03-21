package qvalue

import (
	"time"

	"go.temporal.io/sdk/log"
)

// Bigquery will not allow timestamp if it is less than 1AD and more than 9999AD
func DisallowedTimestamp(dwh QDWHType, t time.Time, logger log.Logger) bool {
	if dwh == QDWHTypeBigQuery {
		year := t.Year()
		if year < 1 || year > 9999 {
			logger.Warn("Nulling Timestamp value for BigQuery as it exceeds allowed range",
				"timestamp", t.String())
			return true
		}
	}
	return false
}
