package qvalue

import (
	"time"

	"go.temporal.io/sdk/log"
)

// Bigquery will not allow timestamp if it is less than 1AD and more than 9999AD
func DisallowedTimestamp(dwh QDWHType, t time.Time, logger log.Logger) bool {
	tMicro := t.UnixMicro()
	if tMicro < 0 || tMicro > 253402300799999999 { // 9999-12-31 23:59:59.999999
		logger.Warn("Nulling Timestamp value for BigQuery as it exceeds allowed range",
			"timestamp", t.String())
		return true
	}
	return false
}
