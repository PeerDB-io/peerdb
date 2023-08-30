package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	"go.temporal.io/sdk/activity"
)

func LogPullMetrics(
	ctx context.Context,
	flowJobName string,
	recordBatch *model.RecordBatch,
	totalRecordsAtSource int64,
) {
	if ctx.Value(shared.EnableMetricsKey) != true {
		return
	}

	metricsHandler := activity.GetMetricsHandler(ctx)
	insertRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.insert_records_pulled", flowJobName))
	updateRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.update_records_pulled", flowJobName))
	deleteRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.delete_records_pulled", flowJobName))
	totalRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.total_records_pulled", flowJobName))
	totalRecordsAtSourceGauge := metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_at_source", flowJobName))

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
}

func LogSyncMetrics(ctx context.Context, flowJobName string, recordsCount int64, duration time.Duration) {
	if ctx.Value(shared.EnableMetricsKey) != true {
		return
	}

	metricsHandler := activity.GetMetricsHandler(ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_synced_per_second", flowJobName))
	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
}

func LogNormalizeMetrics(
	ctx context.Context,
	flowJobName string,
	recordsCount int64,
	duration time.Duration,
	totalRecordsAtTarget int64,
) {
	if ctx.Value(shared.EnableMetricsKey) != true {
		return
	}

	metricsHandler := activity.GetMetricsHandler(ctx)
	recordsNormalizedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_normalized_per_second", flowJobName))
	totalRecordsAtTargetGauge :=
		metricsHandler.Gauge(fmt.Sprintf("cdcflow.%s.records_at_target", flowJobName))

	recordsNormalizedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
	totalRecordsAtTargetGauge.Update(float64(totalRecordsAtTarget))
}

func LogQRepPullMetrics(ctx context.Context, flowJobName string,
	numRecords int, totalRecordsAtSource int64) {
	if ctx.Value(shared.EnableMetricsKey) != true {
		return
	}

	metricsHandler := activity.GetMetricsHandler(ctx)
	totalRecordsPulledGauge := metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.total_records_pulled", flowJobName))
	totalRecordsAtSourceGauge := metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_at_source", flowJobName))

	totalRecordsPulledGauge.Update(float64(numRecords))
	totalRecordsAtSourceGauge.Update(float64(totalRecordsAtSource))
}

func LogQRepSyncMetrics(ctx context.Context, flowJobName string, recordsCount int64, duration time.Duration) {
	if ctx.Value(shared.EnableMetricsKey) != true {
		return
	}

	metricsHandler := activity.GetMetricsHandler(ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_synced_per_second", flowJobName))
	recordsSyncedPerSecondGauge.Update(float64(recordsCount) / duration.Seconds())
}

func LogQRepNormalizeMetrics(ctx context.Context, flowJobName string,
	normalizedRecordsCount int64, duration time.Duration, totalRecordsAtTarget int64) {
	if ctx.Value(shared.EnableMetricsKey) != true {
		return
	}

	metricsHandler := activity.GetMetricsHandler(ctx)
	recordsSyncedPerSecondGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_normalized_per_second", flowJobName))
	totalRecordsAtTargetGauge :=
		metricsHandler.Gauge(fmt.Sprintf("qrepflow.%s.records_at_target", flowJobName))

	recordsSyncedPerSecondGauge.Update(float64(normalizedRecordsCount) / duration.Seconds())
	totalRecordsAtTargetGauge.Update(float64(totalRecordsAtTarget))
}
