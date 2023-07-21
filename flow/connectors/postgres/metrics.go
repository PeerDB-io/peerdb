package connpostgres

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/model"
	"go.temporal.io/sdk/activity"
)

func (c *PostgresConnector) logPullMetrics(flowJobName string, recordBatch *model.RecordBatch,
	sourceTables []string) error {
	metricsHandler := activity.GetMetricsHandler(c.ctx)
	insertRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.insert_records_pulled", flowJobName))
	updateRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.update_records_pulled", flowJobName))
	deleteRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.delete_records_pulled", flowJobName))
	totalRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.total_records_pulled", flowJobName))
	totalRecordsAtSourceGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_at_source", flowJobName))
	totalRecordsAtSource, err := c.getTableCounts(sourceTables)
	if err != nil {
		return err
	}

	insertRecords, updateRecords, deleteRecords := 0, 0, 0
	for _, record := range recordBatch.Records {
		switch record.(type) {
		case *model.InsertRecord:
			insertRecords++
		case *model.UpdateRecord:
			updateRecords++
		case *model.DeleteRecord:
			deleteRecords++
		}
	}

	insertRecordsPulledGauge.Update(float64(insertRecords))
	updateRecordsPulledGauge.Update(float64(updateRecords))
	deleteRecordsPulledGauge.Update(float64(deleteRecords))
	totalRecordsPulledGauge.Update(float64(len(recordBatch.Records)))
	totalRecordsAtSourceGauge.Update(float64(totalRecordsAtSource))

	return nil
}

func (c *PostgresConnector) logSyncMetrics(flowJobName string, recordsCount int64, duration time.Duration) {
	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_synced_per_second", flowJobName))
	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
}

func (c *PostgresConnector) logNormalizeMetrics(flowJobName string, recordsCount int64, duration time.Duration,
	targetTables []string) error {
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

func (c *PostgresConnector) logQRepPullMetrics(flowJobName string, recordBatch *model.QRecordBatch,
	watermarkTable string) error {
	metricsHandler := activity.GetMetricsHandler(c.ctx)
	totalRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.total_records_pulled", flowJobName))
	totalRecordsAtSourceGauge := metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_at_source", flowJobName))
	totalRecordsAtSource, err := c.getTableCounts([]string{watermarkTable})
	if err != nil {
		return err
	}

	totalRecordsPulledGauge.Update(float64(len(recordBatch.Records)))
	totalRecordsAtSourceGauge.Update(float64(totalRecordsAtSource))
	return nil
}

func (c *PostgresConnector) logQRepSyncMetrics(flowJobName string, recordsCount int64, duration time.Duration) {
	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_synced_per_second", flowJobName))
	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
}

func (c *PostgresConnector) logQRepNormalizeMetrics(flowJobName string, recordsCount int64, duration time.Duration,
	tableName string) error {
	metricsHandler := activity.GetMetricsHandler(c.ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_normalized_per_second", flowJobName))
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
