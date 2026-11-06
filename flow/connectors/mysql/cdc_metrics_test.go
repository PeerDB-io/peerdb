package connmysql

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMysqlPullCountersFlush(t *testing.T) {
	ctx := t.Context()
	om, reader := newMetricsTestOtelManager(t)
	for name, counter := range map[string]*metric.Int64Counter{
		"charsets":    &om.Metrics.UsedMySQLCharsetsCounter,
		"fetched":     &om.Metrics.FetchedBytesCounter,
		"all_fetched": &om.Metrics.AllFetchedBytesCounter,
		"receive":     &om.Metrics.CDCReceiveTimeCounter,
		"process":     &om.Metrics.CDCProcessTimeCounter,
		"add_record":  &om.Metrics.CDCAddRecordTimeCounter,
	} {
		var err error
		*counter, err = om.Meter.Int64Counter(name)
		require.NoError(t, err)
	}
	counters := newMysqlPullCounters(om)
	counters.recordUsedCharset("utf8mb4", "not_transcoded")
	counters.recordUsedCharset("utf8mb4", "not_transcoded")
	counters.fetchedBytes.Add(100)
	counters.totalFetchedBytes.Add(100)
	counters.allFetchedBytes.Add(200)
	counters.receiveTime.Add(3)
	counters.processTime.Add(4)
	counters.addRecordTime.Add(5)
	counters.flush(ctx)
	// A final flush after the interval must not double-count the same observations.
	counters.flush(ctx)
	values := collectInt64Metrics(t, ctx, reader)
	require.Equal(t, map[string]int64{
		"charsets": 2, "fetched": 100, "all_fetched": 200,
		"receive": 3, "process": 4, "add_record": 5,
	}, values)
	require.Equal(t, int64(100), counters.totalFetchedBytes.Load())
}

func TestMysqlPullGauges(t *testing.T) {
	ctx := t.Context()
	om, reader := newMetricsTestOtelManager(t)
	var err error
	om.Metrics.SourceLagGauge, err = om.Meter.Int64Gauge("source_lag")
	require.NoError(t, err)
	om.Metrics.LatestConsumedLogEventGauge, err = om.Meter.Int64Gauge("latest_consumed")
	require.NoError(t, err)
	at := func(seconds float64) time.Time {
		return time.Unix(0, int64(seconds*float64(time.Second)))
	}
	observe := func(g *mysqlPullGauges, timestamp uint32, receivedAt float64) {
		g.observeEvent(&replication.BinlogEvent{
			Header: &replication.EventHeader{Timestamp: timestamp},
			Event:  &replication.XIDEvent{},
		}, at(receivedAt))
	}

	gauges := newMysqlPullGauges(om, 2*time.Second)
	// A batch without events records nothing at batch end.
	gauges.recordBatchEnd(ctx)
	values := collectInt64Metrics(t, ctx, reader)
	require.Empty(t, values)

	observe(gauges, 100, 100)
	gauges.observeConsumed(100)
	gauges.recordPeriodic(ctx, at(100))
	values = collectInt64Metrics(t, ctx, reader)
	require.Equal(t, map[string]int64{"source_lag": 2000, "latest_consumed": 100}, values)

	// Within the throttle window nothing is recorded, and the latest value wins over the max.
	observe(gauges, 99, 100.5)
	gauges.recordPeriodic(ctx, at(100.5))
	observe(gauges, 100, 100.8)
	observe(gauges, 0, 100.9)
	gauges.observeConsumed(0)
	gauges.recordPeriodic(ctx, at(100.9))
	values = collectInt64Metrics(t, ctx, reader)
	require.Equal(t, map[string]int64{"source_lag": 2000, "latest_consumed": 100}, values)

	// Batch end records the pending lag; the consumed timestamp was already recorded.
	gauges.recordBatchEnd(ctx)
	values = collectInt64Metrics(t, ctx, reader)
	require.Equal(t, map[string]int64{"source_lag": 2800, "latest_consumed": 100}, values)
	require.False(t, gauges.sourceLagPending)
	require.Zero(t, gauges.latestConsumedTimestamp)

	gauges.observeEvent(&replication.BinlogEvent{Event: &replication.HeartbeatEvent{}}, at(200))
	gauges.recordPeriodic(ctx, at(200))
	values = collectInt64Metrics(t, ctx, reader)
	require.Equal(t, int64(0), values["source_lag"])

	gauges.observeEvent(&replication.BinlogEvent{
		Header: &replication.EventHeader{Timestamp: 600},
		Event:  &replication.GTIDEvent{ImmediateCommitTimestamp: 600_500_000},
	}, at(601))
	gauges.recordPeriodic(ctx, at(601))
	values = collectInt64Metrics(t, ctx, reader)
	require.Equal(t, int64(2500), values["source_lag"])
}

func collectInt64Metrics(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var result metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &result))
	values := make(map[string]int64)
	for _, scope := range result.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			switch data := measurement.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					values[measurement.Name] += point.Value
				}
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					values[measurement.Name] = point.Value
				}
			}
		}
	}
	return values
}
