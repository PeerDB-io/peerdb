package connsnowflake

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/shared"
	"go.temporal.io/sdk/activity"
)

func (c *SnowflakeConnector) logSyncMetrics(flowJobName string, recordsCount int64, duration time.Duration) {
	if c.ctx.Value(shared.EnableMetricsKey) != true {
		return
	}

	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_synced_per_second", flowJobName))
	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
}

func (c *SnowflakeConnector) logNormalizeMetrics(flowJobName string, recordsCount int64, duration time.Duration,
	targetTables []string) error {
	if c.ctx.Value(shared.EnableMetricsKey) != true {
		return nil
	}

	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsNormalizedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_normalized_per_second", flowJobName))
	totalRecordsAtTargetGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_at_target", flowJobName))
	totalRecordsAtTarget, err := c.getTableCounts(targetTables)
	if err != nil {
		return err
	}

	recordsNormalizedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
	totalRecordsAtTargetGauge.Update(float64(totalRecordsAtTarget))

	return nil
}

func (c *SnowflakeConnector) logQRepSyncMetrics(flowJobName string, recordsCount int64,
	duration time.Duration, tableName string) error {
	if c.ctx.Value(shared.EnableMetricsKey) != true {
		return nil
	}

	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_synced_per_second", flowJobName))
	totalRecordsAtTargetGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_at_target", flowJobName))
	totalRecordsAtTarget, err := c.getTableCounts([]string{tableName})
	if err != nil {
		return err
	}

	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
	totalRecordsAtTargetGauge.Update(float64(totalRecordsAtTarget))
	return nil
}

func (c *SnowflakeConnector) logQRepNormalizeMetrics(flowJobName string, recordsCount int64, duration time.Duration) {
	if c.ctx.Value(shared.EnableMetricsKey) != true {
		return
	}

	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_normalized_per_second", flowJobName))

	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
}
