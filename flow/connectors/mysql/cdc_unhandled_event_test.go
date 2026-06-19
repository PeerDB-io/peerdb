package connmysql

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
)

type capturedLogger struct {
	warnings []capturedLog
}

type capturedLog struct {
	msg     string
	keyvals []interface{}
}

func (l *capturedLogger) Debug(string, ...interface{}) {}

func (l *capturedLogger) Info(string, ...interface{}) {}

func (l *capturedLogger) Warn(msg string, keyvals ...interface{}) {
	l.warnings = append(l.warnings, capturedLog{msg: msg, keyvals: keyvals})
}

func (l *capturedLogger) Error(string, ...interface{}) {}

func newUnhandledBinlogEventTestMetrics(t *testing.T) (*otel_metrics.OtelManager, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	counter, err := otel_metrics.NewContextAwareInt64Counter(
		provider.Meter("io.peerdb.test"),
		otel_metrics.BuildMetricName(otel_metrics.UnhandledBinlogEventCounterName),
	)
	require.NoError(t, err)

	return &otel_metrics.OtelManager{
		Metrics: otel_metrics.Metrics{
			UnhandledBinlogEventCounter: counter,
		},
	}, reader
}

func collectUnhandledBinlogEventCounts(
	ctx context.Context,
	t *testing.T,
	reader *sdkmetric.ManualReader,
) map[string]int64 {
	t.Helper()

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &metrics))

	counts := make(map[string]int64)
	for _, scopeMetrics := range metrics.ScopeMetrics {
		for _, metric := range scopeMetrics.Metrics {
			if metric.Name != otel_metrics.BuildMetricName(otel_metrics.UnhandledBinlogEventCounterName) {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok, "unexpected metric data type %T", metric.Data)
			for _, point := range sum.DataPoints {
				value, ok := point.Attributes.Value(otel_metrics.EventTypeKey)
				require.True(t, ok, "missing event type attribute")
				counts[value.AsString()] += point.Value
			}
		}
	}
	return counts
}

func TestReportUnhandledBinlogEventCountsGenericEventAndWarnsOnce(t *testing.T) {
	ctx := t.Context()
	otelManager, reader := newUnhandledBinlogEventTestMetrics(t)
	logger := &capturedLogger{}
	connector := &MySqlConnector{logger: logger}
	warningReported := false

	event := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.EventType(255)},
		Event:  &replication.GenericEvent{},
	}
	connector.reportUnhandledBinlogEvent(ctx, otelManager, event, &warningReported)
	connector.reportUnhandledBinlogEvent(ctx, otelManager, event, &warningReported)

	counts := collectUnhandledBinlogEventCounts(ctx, t, reader)
	require.Equal(t, int64(2), counts["UnknownEvent"])
	require.Len(t, logger.warnings, 1)
	require.Equal(t, "[mysql] unhandled binlog event type", logger.warnings[0].msg)
}

func TestReportUnhandledBinlogEventSkipsExpectedBenignEvents(t *testing.T) {
	ctx := t.Context()
	otelManager, reader := newUnhandledBinlogEventTestMetrics(t)
	logger := &capturedLogger{}
	connector := &MySqlConnector{logger: logger}
	warningReported := false

	for _, eventType := range []replication.EventType{
		replication.FORMAT_DESCRIPTION_EVENT,
		replication.INTVAR_EVENT,
		replication.TABLE_MAP_EVENT,
		replication.HEARTBEAT_EVENT,
		replication.ROWS_QUERY_EVENT,
		replication.PREVIOUS_GTIDS_EVENT,
		replication.HEARTBEAT_LOG_EVENT_V2,
		replication.GTID_TAGGED_LOG_EVENT,
		replication.MARIADB_ANNOTATE_ROWS_EVENT,
		replication.MARIADB_BINLOG_CHECKPOINT_EVENT,
		replication.MARIADB_GTID_EVENT,
		replication.MARIADB_GTID_LIST_EVENT,
	} {
		connector.reportUnhandledBinlogEvent(ctx, otelManager, &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: eventType},
		}, &warningReported)
	}

	require.Empty(t, collectUnhandledBinlogEventCounts(ctx, t, reader))
	require.Empty(t, logger.warnings)
	require.False(t, warningReported)
}

func TestReportUnhandledBinlogEventCountsUnhandledRowsEventType(t *testing.T) {
	ctx := t.Context()
	otelManager, reader := newUnhandledBinlogEventTestMetrics(t)
	logger := &capturedLogger{}
	connector := &MySqlConnector{logger: logger}
	warningReported := false

	connector.reportUnhandledBinlogEvent(ctx, otelManager, &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.PARTIAL_UPDATE_ROWS_EVENT},
		Event:  &replication.RowsEvent{},
	}, &warningReported)

	counts := collectUnhandledBinlogEventCounts(ctx, t, reader)
	require.Equal(t, int64(1), counts["PartialUpdateRowsEvent"])
	require.Len(t, logger.warnings, 1)
}
