package connbigquery

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

func (c *BigQueryConnector) logSyncMetrics(flowJobName string, recordsCount int64, duration time.Duration) {
	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_synced_per_second", flowJobName))
	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
}

func (c *BigQueryConnector) logQRepSyncMetrics(flowJobName string, recordsCount int64, duration time.Duration) {
	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_synced_per_second", flowJobName))
	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
}

// TODO: currently unable to find a way to retrieve records modified by a DML operation.
func (c *BigQueryConnector) logNormalizeMetrics(flowJobName string, targetTables []string) error {
	metricsHandler := activity.GetMetricsHandler(c.ctx)
	totalRecordsAtTargetGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_at_target", flowJobName))
	totalRecordsAtTarget, err := c.getTableCounts(targetTables)
	if err != nil {
		return err
	}

	totalRecordsAtTargetGauge.Update(float64(totalRecordsAtTarget))

	return nil
}
