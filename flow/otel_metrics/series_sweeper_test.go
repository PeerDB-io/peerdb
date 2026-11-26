package otel_metrics

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type sweepHarness struct {
	sweeper   *seriesSweeper
	exporter  *exportWatermarkExporter
	reader    *sdkmetric.ManualReader
	counter   *ContextAwareInt64Counter
	gauge     *contextAwareInt64Gauge
	histogram *ContextAwareInt64Histogram
}

func newSweepHarness(t *testing.T) *sweepHarness {
	t.Helper()
	meter, reader := newTestMeter(t)
	sweeper := &seriesSweeper{droppedFlows: make(map[string]int64)}
	counter, err := NewContextAwareInt64Counter(meter, "test_counter", withSweeper(sweeper))
	require.NoError(t, err)
	gauge, err := ContextAwareInt64Gauge(meter, "test_gauge", withSweeper(sweeper))
	require.NoError(t, err)
	histogram, err := NewContextAwareInt64Histogram(meter, "test_histogram", withSweeper(sweeper))
	require.NoError(t, err)
	return &sweepHarness{
		sweeper:   sweeper,
		exporter:  &exportWatermarkExporter{Exporter: &fakeExporter{}, watermark: &sweeper.exportWatermark},
		reader:    reader,
		counter:   counter.(*ContextAwareInt64Counter),
		gauge:     gauge.(*contextAwareInt64Gauge),
		histogram: histogram.(*ContextAwareInt64Histogram),
	}
}

func (h *sweepHarness) record(ctx context.Context, value int64) {
	h.counter.Add(ctx, value)
	h.gauge.Record(ctx, value)
	h.histogram.Record(ctx, value)
}

// export collects a round and exports it, as the periodic reader does.
func (h *sweepHarness) export(t *testing.T) *metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, h.reader.Collect(t.Context(), &rm))
	require.NoError(t, h.exporter.Export(t.Context(), &rm))
	return &rm
}

// exportAfterGracePeriod exports like export, but as if that export started exportWatermarkBuffer from now, so
// flows already recorded as dropped are past their grace period.
func (h *sweepHarness) exportAfterGracePeriod(t *testing.T) *metricdata.ResourceMetrics {
	t.Helper()
	rm := h.export(t)
	h.sweeper.exportWatermark.Store(monotonicNanos() + int64(exportWatermarkBuffer))
	return rm
}

// valuesByFlow returns the value of each int64 sum or gauge point of the named metric by its flowName;
// "" is the key of a point without one.
func valuesByFlow(t *testing.T, rm *metricdata.ResourceMetrics, name string) map[string]int64 {
	t.Helper()
	values := make(map[string]int64)
	add := func(point metricdata.DataPoint[int64]) {
		flowName, _ := point.Attributes.Value(FlowNameKey)
		values[flowName.AsString()] = point.Value
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					add(point)
				}
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					add(point)
				}
			default:
				t.Fatalf("unexpected data type %T", m.Data)
			}
		}
	}
	return values
}

func countEntries(m *sync.Map) int {
	n := 0
	m.Range(func(any, any) bool {
		n++
		return true
	})
	return n
}

func TestSweepDeletesDroppedFlowSeriesAfterGracePeriod(t *testing.T) {
	h := newSweepHarness(t)
	h.record(flowContext("live"), 1)
	h.record(flowContext("dropped"), 2)
	h.record(context.Background(), 3)

	// The first sweep that misses a flow only records it as dropped.
	require.Zero(t, h.sweeper.sweep([]string{"live"}))
	require.Contains(t, h.sweeper.droppedFlows, "dropped")
	rm := h.export(t)
	require.Equal(t, map[string]int64{"live": 1, "dropped": 2, "": 3}, valuesByFlow(t, rm, "test_counter"))
	// An export soon after is not enough: late updates from the drop may still be coming.
	require.Zero(t, h.sweeper.sweep([]string{"live"}))

	h.exportAfterGracePeriod(t)
	require.Equal(t, 2, h.sweeper.sweep([]string{"live"}), "the dropped flow's counter and gauge series")
	require.NotContains(t, h.sweeper.droppedFlows, "dropped")

	h.record(flowContext("live"), 1)
	h.record(context.Background(), 1)
	rm = h.export(t)
	require.Equal(t, map[string]int64{"live": 2, "": 4}, valuesByFlow(t, rm, "test_counter"))
	require.Equal(t, map[string]int64{"live": 1, "": 1}, valuesByFlow(t, rm, "test_gauge"))
	require.Equal(t, 2, countEntries(&h.counter.series))
	require.Equal(t, 2, countEntries(&h.gauge.gauge.series))
}

func TestSweepExportsLateUpdatesOfDroppedFlow(t *testing.T) {
	h := newSweepHarness(t)
	ctx := flowContext("dropped")
	h.record(ctx, 1)
	require.Zero(t, h.sweeper.sweep(nil))

	// An update after the flow was dropped is exported before the series goes away.
	h.record(ctx, 10)
	rm := h.exportAfterGracePeriod(t)
	require.Equal(t, map[string]int64{"dropped": 11}, valuesByFlow(t, rm, "test_counter"))
	require.Equal(t, map[string]int64{"dropped": 10}, valuesByFlow(t, rm, "test_gauge"))
	require.Equal(t, 2, h.sweeper.sweep(nil))
}

func TestSweepRestartsGracePeriodOfRecreatedFlow(t *testing.T) {
	h := newSweepHarness(t)
	ctx := flowContext("recreated")
	h.record(ctx, 1)
	require.Zero(t, h.sweeper.sweep(nil))
	h.exportAfterGracePeriod(t)

	// The flow is back before its series were deleted, so its old dropped time is forgotten.
	require.Zero(t, h.sweeper.sweep([]string{"recreated"}))
	require.NotContains(t, h.sweeper.droppedFlows, "recreated")

	// Dropped again: it gets a new grace period instead of being deleted right away.
	require.Zero(t, h.sweeper.sweep(nil))
	h.export(t)
	require.Zero(t, h.sweeper.sweep(nil))
	h.exportAfterGracePeriod(t)
	require.Equal(t, 2, h.sweeper.sweep(nil))
}

func TestSweepKeepsGaugeValuePendingExport(t *testing.T) {
	h := newSweepHarness(t)
	h.gauge.Record(flowContext("dropped"), 1)
	require.Zero(t, h.sweeper.sweep(nil))
	h.exportAfterGracePeriod(t)
	// A value recorded after the export, as when a Record races the sweep, is still pending.
	h.gauge.gauge.series.Range(func(_, value any) bool {
		value.(*gaugeSeries).pending.Store(true)
		return true
	})
	require.Zero(t, h.sweeper.sweep(nil))
	require.Equal(t, map[string]int64{"dropped": 1}, valuesByFlow(t, h.export(t), "test_gauge"))

	// The flow was forgotten with its other series, so it gets a new grace period.
	require.Zero(t, h.sweeper.sweep(nil))
	h.exportAfterGracePeriod(t)
	require.Equal(t, 1, h.sweeper.sweep(nil))
}

func TestSweepInvalidatesCachedSeries(t *testing.T) {
	h := newSweepHarness(t)
	ctx := flowContext("recreated")
	// Explicit flowName on a context without flow metadata, cached under the "" entry.
	explicit := metric.WithAttributeSet(attribute.NewSet(attribute.String(FlowNameKey, "explicit")))
	h.counter.Add(ctx, 5)
	h.counter.Add(context.Background(), 7, explicit)
	require.Zero(t, h.sweeper.sweep(nil))
	h.exportAfterGracePeriod(t)
	require.Equal(t, 2, h.sweeper.sweep(nil))

	// The same contexts must resolve new series, not keep adding to the deleted ones.
	h.counter.Add(ctx, 1)
	h.counter.Add(context.Background(), 2, explicit)
	require.Equal(t, map[string]int64{"recreated": 1, "explicit": 2}, valuesByFlow(t, h.export(t), "test_counter"))
}

// A pointer cached concurrently with a sweep's prune can still point to a deleted series; get must resolve again.
func TestDeletedSeriesCachedAfterPruneIsResolvedAgain(t *testing.T) {
	h := newSweepHarness(t)
	ctx := flowContext("flow")
	h.counter.Add(ctx, 5)
	h.counter.series.Range(func(key, value any) bool {
		value.(*counterSeries).deleted.Store(true)
		h.counter.series.Delete(key)
		return true
	})

	h.counter.Add(ctx, 1)
	require.Equal(t, map[string]int64{"flow": 1}, valuesByFlow(t, h.export(t), "test_counter"))
}

func TestSweepPrunesContextEntriesOfDroppedFlows(t *testing.T) {
	h := newSweepHarness(t)
	h.record(flowContext("live"), 1)
	h.record(flowContext("dropped"), 1)
	h.record(context.Background(), 1)
	h.sweeper.sweep([]string{"live"})
	h.exportAfterGracePeriod(t)
	h.sweeper.sweep([]string{"live"})

	for name, entries := range map[string]*sync.Map{
		"counter":   &h.counter.contexts.entries,
		"gauge":     &h.gauge.gauge.contexts.entries,
		"histogram": &h.histogram.contexts.entries,
	} {
		_, ok := entries.Load("dropped")
		require.False(t, ok, name)
		_, ok = entries.Load("live")
		require.True(t, ok, name)
		_, ok = entries.Load("")
		require.True(t, ok, name)
	}
}

type fakeExporter struct {
	sdkmetric.Exporter
	err error
}

func (e *fakeExporter) Export(context.Context, *metricdata.ResourceMetrics) error {
	return e.err
}

func TestExportWatermarkExporter(t *testing.T) {
	inner := &fakeExporter{}
	sweeper := &seriesSweeper{}
	exporter := &exportWatermarkExporter{Exporter: inner, watermark: &sweeper.exportWatermark}
	export := func() error { return exporter.Export(t.Context(), &metricdata.ResourceMetrics{}) }

	before := monotonicNanos()
	require.NoError(t, export())
	watermark := sweeper.exportWatermark.Load()
	require.GreaterOrEqual(t, watermark, before, "the watermark is when the export started")
	require.LessOrEqual(t, watermark, monotonicNanos())

	inner.err = errors.New("collector unavailable")
	require.Error(t, export())
	require.Equal(t, watermark, sweeper.exportWatermark.Load(), "a failed export leaves the watermark")
}

// Sweeps race with updates and exports; the live flow's counter must never lose an increment.
func TestSweepConcurrentWithUpdatesAndExports(t *testing.T) {
	h := newSweepHarness(t)
	const rounds = 2000
	live, dropped := flowContext("live"), flowContext("dropped")

	var wg sync.WaitGroup
	wg.Go(func() {
		for range rounds {
			h.record(live, 1)
			h.record(dropped, 1)
		}
	})
	wg.Go(func() {
		for range rounds / 10 {
			h.exportAfterGracePeriod(t)
			h.sweeper.sweep([]string{"live"})
		}
	})
	wg.Wait()

	require.Equal(t, int64(rounds), valuesByFlow(t, h.export(t), "test_counter")["live"])
}
