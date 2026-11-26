package otel_metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) advance() time.Time {
	c.now = c.now.Add(time.Minute)
	return c.now
}

func newTestLiveFlowsFilter() *liveFlowsFilter {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	filter := newLiveFlowsFilter()
	filter.now = func() time.Time { return clock.advance() }
	return filter
}

func listing(names ...string) FlowNamesLister {
	return func(context.Context) ([]string, error) { return names, nil }
}

type recordedMeter struct {
	counter   metric.Int64Counter
	gauge     metric.Int64Gauge
	histogram metric.Int64Histogram
	reader    *sdkmetric.ManualReader
}

func newRecordedMeter(t *testing.T) *recordedMeter {
	t.Helper()
	meter, reader := newTestMeter(t)
	counter, err := NewContextAwareInt64Counter(meter, "test_counter")
	require.NoError(t, err)
	gauge, err := ContextAwareInt64Gauge(meter, "test_gauge")
	require.NoError(t, err)
	histogram, err := NewContextAwareInt64Histogram(meter, "test_histogram")
	require.NoError(t, err)
	return &recordedMeter{counter: counter, gauge: gauge, histogram: histogram, reader: reader}
}

func (m *recordedMeter) record(ctx context.Context) {
	m.counter.Add(ctx, 1)
	m.gauge.Record(ctx, 1)
	m.histogram.Record(ctx, 1)
}

func (m *recordedMeter) collect(t *testing.T) *metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, m.reader.Collect(t.Context(), &rm))
	return &rm
}

// flowNamesByMetric returns the flowName of each data point by metric name; "" marks a point without one.
func flowNamesByMetric(t *testing.T, rm *metricdata.ResourceMetrics) map[string][]string {
	t.Helper()
	flowName := func(attrs attribute.Set) string {
		value, _ := attrs.Value(FlowNameKey)
		return value.AsString()
	}
	result := make(map[string][]string)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					result[m.Name] = append(result[m.Name], flowName(point.Attributes))
				}
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					result[m.Name] = append(result[m.Name], flowName(point.Attributes))
				}
			case metricdata.Histogram[int64]:
				for _, point := range data.DataPoints {
					result[m.Name] = append(result[m.Name], flowName(point.Attributes))
				}
			default:
				t.Fatalf("unexpected data type %T", m.Data)
			}
		}
	}
	return result
}

func TestLiveFlowsFilterPassesEverythingWithoutSnapshot(t *testing.T) {
	filter := newTestLiveFlowsFilter()
	meter := newRecordedMeter(t)
	meter.record(flowContext("dropped"))

	rm := meter.collect(t)
	require.Same(t, rm, filter.filter(rm))
}

func TestLiveFlowsFilterDropsFlowsMissingFromLaterSnapshot(t *testing.T) {
	filter := newTestLiveFlowsFilter()
	meter := newRecordedMeter(t)
	require.NoError(t, filter.refresh(t.Context(), listing("live")))

	meter.record(flowContext("live"))
	meter.record(flowContext("dropped"))
	meter.record(context.Background())

	// The first export after the snapshot keeps the missing flow, which may have been created after it.
	all := []string{"live", "dropped", ""}
	require.ElementsMatch(t, all, flowNamesByMetric(t, filter.filter(meter.collect(t)))["test_counter"])

	require.NoError(t, filter.refresh(t.Context(), listing("live")))
	meter.record(flowContext("live"))
	meter.record(flowContext("dropped"))
	meter.record(context.Background())
	rm := meter.collect(t)
	byMetric := flowNamesByMetric(t, filter.filter(rm))
	for _, name := range []string{"test_counter", "test_gauge", "test_histogram"} {
		require.ElementsMatch(t, []string{"live", ""}, byMetric[name], name)
	}
	// The collected data is left untouched, since the SDK reuses it.
	require.ElementsMatch(t, all, flowNamesByMetric(t, rm)["test_counter"])
}

func TestLiveFlowsFilterKeepsFlowCreatedAfterSnapshot(t *testing.T) {
	filter := newTestLiveFlowsFilter()
	meter := newRecordedMeter(t)
	require.NoError(t, filter.refresh(t.Context(), listing()))

	meter.record(flowContext("new"))
	for range 3 {
		require.Equal(t, []string{"new"}, flowNamesByMetric(t, filter.filter(meter.collect(t)))["test_counter"])
	}

	require.NoError(t, filter.refresh(t.Context(), listing("new")))
	require.Equal(t, []string{"new"}, flowNamesByMetric(t, filter.filter(meter.collect(t)))["test_counter"])
	require.Empty(t, filter.missingSince)
}

func TestLiveFlowsFilterRemovesEmptiedMetrics(t *testing.T) {
	filter := newTestLiveFlowsFilter()
	meter := newRecordedMeter(t)
	require.NoError(t, filter.refresh(t.Context(), listing()))
	meter.record(flowContext("dropped"))
	filter.filter(meter.collect(t))
	require.NoError(t, filter.refresh(t.Context(), listing()))

	meter.record(flowContext("dropped"))
	require.Empty(t, filter.filter(meter.collect(t)).ScopeMetrics)
}

func TestLiveFlowsFilterKeepsSnapshotOnRefreshFailure(t *testing.T) {
	filter := newTestLiveFlowsFilter()
	require.NoError(t, filter.refresh(t.Context(), listing("live")))
	names, takenAt := filter.names, filter.takenAt

	require.Error(t, filter.refresh(t.Context(), func(context.Context) ([]string, error) {
		return nil, errors.New("catalog unavailable")
	}))
	require.Equal(t, names, filter.names)
	require.Equal(t, takenAt, filter.takenAt)
}

type capturingExporter struct {
	sdkmetric.Exporter
	exported *metricdata.ResourceMetrics
}

func (e *capturingExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	e.exported = rm
	return nil
}

func TestFlowFilteringExporter(t *testing.T) {
	filter := newTestLiveFlowsFilter()
	meter := newRecordedMeter(t)
	require.NoError(t, filter.refresh(t.Context(), listing("live")))
	meter.record(flowContext("live"))
	meter.record(flowContext("dropped"))
	filter.filter(meter.collect(t))
	require.NoError(t, filter.refresh(t.Context(), listing("live")))

	inner := &capturingExporter{}
	exporter := &flowFilteringExporter{Exporter: inner, filter: filter}
	require.NoError(t, exporter.Export(t.Context(), meter.collect(t)))
	require.Equal(t, []string{"live"}, flowNamesByMetric(t, inner.exported)["test_counter"])
}
