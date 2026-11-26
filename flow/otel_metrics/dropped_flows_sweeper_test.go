package otel_metrics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type sweepTest struct {
	sweeper    *droppedFlowsSweeper
	reader     *sdkmetric.ManualReader
	counter    *ContextAwareInt64Counter
	gauge      *contextAwareInt64Gauge
	floatGauge *contextAwareFloat64Gauge
	// now is the clock of the test's sweeps and exports.
	now time.Time
}

func newSweepTest(t *testing.T) *sweepTest {
	t.Helper()
	meter, reader := newTestMeter(t)
	sweeper := &droppedFlowsSweeper{}
	counter, err := NewContextAwareInt64Counter(meter, "test_counter")
	require.NoError(t, err)
	gauge, err := ContextAwareInt64Gauge(meter, "test_gauge")
	require.NoError(t, err)
	floatGauge, err := ContextAwareFloat64Gauge(meter, "test_float_gauge")
	require.NoError(t, err)
	for _, instrument := range []any{counter, gauge, floatGauge} {
		sweeper.instruments = append(sweeper.instruments, instrument.(sweepableInstrument))
	}
	return &sweepTest{
		sweeper:    sweeper,
		reader:     reader,
		counter:    counter.(*ContextAwareInt64Counter),
		gauge:      gauge.(*contextAwareInt64Gauge),
		floatGauge: floatGauge.(*contextAwareFloat64Gauge),
		now:        time.Now(),
	}
}

func (s *sweepTest) record(ctx context.Context, value int64) {
	s.counter.Add(ctx, value)
	s.gauge.Record(ctx, value)
	s.floatGauge.Record(ctx, float64(value))
}

func (s *sweepTest) advance(d time.Duration) {
	s.now = s.now.Add(d)
}

func (s *sweepTest) sweep(liveFlows ...string) int {
	return s.sweeper.sweep(liveFlows, s.now)
}

// deliver records a successful export that started now.
func (s *sweepTest) deliver() {
	deliveredAt := s.now
	s.sweeper.lastDeliveredAt.Store(&deliveredAt)
}

// flowValues collects a round and returns the named metric's int64 values by flowName, "" for none.
func (s *sweepTest) flowValues(t *testing.T, name string) map[string]int64 {
	t.Helper()
	values := make(map[string]int64)
	for _, point := range collectInt64Points(t, s.reader, name) {
		flowName, _ := point.attrs.Value(FlowNameKey)
		values[flowName.AsString()] = point.value
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

func TestSweepDeletesDroppedFlowAfterDeliveryPastBuffer(t *testing.T) {
	s := newSweepTest(t)
	s.record(flowContext("live"), 1)
	s.record(flowContext("dropped"), 2)
	s.record(context.Background(), 3)
	s.deliver()

	require.Zero(t, s.sweep("live"), "the first sweep only detects the dropped flow")
	require.Contains(t, s.sweeper.detectedAt, "dropped")

	// An export within the buffer after detection is not enough.
	s.advance(droppedFlowsDeliveryBuffer)
	s.deliver()
	s.advance(time.Hour)
	require.Zero(t, s.sweep("live"))

	s.deliver()
	require.Equal(t, 3, s.sweep("live"), "the dropped flow's counter, int64 gauge, and float64 gauge series")
	require.Empty(t, s.sweeper.detectedAt)
	require.Equal(t, 2, countEntries(&s.counter.series))
	require.Equal(t, 2, countEntries(&s.gauge.series))
	require.Equal(t, 2, countEntries(&s.floatGauge.series))

	s.record(flowContext("live"), 1)
	s.record(context.Background(), 1)
	// The gauge first, since any collection consumes its pending values.
	require.Equal(t, map[string]int64{"live": 1, "": 1}, s.flowValues(t, "test_gauge"))
	require.Equal(t, map[string]int64{"live": 2, "": 4}, s.flowValues(t, "test_counter"))
}

func TestSweepClearsContextCachesOnlyWhenDeleting(t *testing.T) {
	s := newSweepTest(t)
	liveCtx := flowContext("live")
	s.record(liveCtx, 1)
	s.record(flowContext("dropped"), 2)

	// Sweep 1 only detects "dropped", so caches stay warm.
	require.Zero(t, s.sweep("live"))
	require.Equal(t, 2, countEntries(&s.counter.contexts.entries))

	// Sweep 2 deletes "dropped" and clears the context caches of counters and gauges.
	s.advance(time.Hour)
	s.deliver()
	require.Equal(t, 3, s.sweep("live"))
	for name, entries := range map[string]*sync.Map{
		"counter":     &s.counter.contexts.entries,
		"gauge":       &s.gauge.contexts.entries,
		"float_gauge": &s.floatGauge.contexts.entries,
	} {
		require.Zero(t, countEntries(entries), name)
	}
	require.Nil(t, s.counter.contexts.last.Load())
	require.Nil(t, s.gauge.contexts.last.Load())
	require.Nil(t, s.floatGauge.contexts.last.Load())

	// Subsequent writes on the live flow repopulate the cache and attach to the existing series.
	s.record(liveCtx, 4)
	require.Equal(t, map[string]int64{"live": 5}, s.flowValues(t, "test_counter"))
}

func TestSweepKeepsDroppedFlowWithoutDelivery(t *testing.T) {
	s := newSweepTest(t)
	s.record(flowContext("dropped"), 1)
	require.Zero(t, s.sweep())

	// Exports keep failing (e.g., collector down > 1h), so lastDeliveredAt stays unset.
	s.advance(2 * time.Hour)
	require.Zero(t, s.sweep())
	require.Equal(t, map[string]int64{"dropped": 1}, s.flowValues(t, "test_counter"))
}

func TestSweepForgetsFlowLiveAgain(t *testing.T) {
	s := newSweepTest(t)
	s.record(flowContext("flow"), 1)
	require.Zero(t, s.sweep())
	require.Contains(t, s.sweeper.detectedAt, "flow")

	s.advance(time.Hour)
	s.deliver()
	require.Zero(t, s.sweep("flow"))
	require.Empty(t, s.sweeper.detectedAt)

	// If the flow is dropped later, detection starts over.
	s.advance(time.Hour)
	s.deliver()
	require.Zero(t, s.sweep())
	require.Equal(t, map[string]int64{"flow": 1}, s.flowValues(t, "test_counter"))
}

func TestSweepResolvesSeriesAgainAfterDeletion(t *testing.T) {
	s := newSweepTest(t)
	ctx := flowContext("dropped")
	s.record(ctx, 5)
	s.sweep()
	s.advance(time.Hour)
	s.deliver()
	require.Equal(t, 3, s.sweep())

	// A late update or recreated flow starts a fresh series.
	s.record(ctx, 1)
	require.Equal(t, map[string]int64{"dropped": 1}, s.flowValues(t, "test_counter"))
}

func TestSweepDeletesSeriesWithExplicitFlowName(t *testing.T) {
	s := newSweepTest(t)
	// Scheduled tasks pass flowName as an attribute on a context without flow metadata, so the series is cached
	// under the context's empty flow name.
	ctx := context.Background()
	explicit := metric.WithAttributeSet(attribute.NewSet(attribute.String(FlowNameKey, "dropped")))
	s.gauge.Record(ctx, 7, explicit)
	s.sweep()
	s.advance(time.Hour)
	s.deliver()
	require.Equal(t, 1, s.sweep())
	require.Zero(t, countEntries(&s.gauge.series))
	require.Zero(t, countEntries(&s.gauge.contexts.entries))

	s.gauge.Record(ctx, 8, explicit)
	require.Equal(t, map[string]int64{"dropped": 8}, s.flowValues(t, "test_gauge"))
}

type failingExporter struct {
	sdkmetric.Exporter
	err error
}

func (e *failingExporter) Export(context.Context, *metricdata.ResourceMetrics) error {
	return e.err
}

func TestDeliveryTrackingExporterRecordsOnlySuccessfulExports(t *testing.T) {
	sweeper := &droppedFlowsSweeper{}
	inner := &failingExporter{err: errors.New("collector unavailable")}
	exporter := &deliveryTrackingExporter{Exporter: inner, lastDeliveredAt: &sweeper.lastDeliveredAt}
	require.Error(t, exporter.Export(t.Context(), &metricdata.ResourceMetrics{}))
	require.Nil(t, sweeper.lastDeliveredAt.Load())

	inner.err = nil
	before := time.Now()
	require.NoError(t, exporter.Export(t.Context(), &metricdata.ResourceMetrics{}))
	deliveredAt := sweeper.lastDeliveredAt.Load()
	require.NotNil(t, deliveredAt)
	require.False(t, deliveredAt.Before(before))
}

func TestSweepConcurrentWithRecords(t *testing.T) {
	s := newSweepTest(t)
	const rounds = 2000
	live, dropped := flowContext("live"), flowContext("dropped")
	s.record(dropped, 1)
	s.sweep("live")
	s.advance(time.Hour)
	s.deliver()

	var wg sync.WaitGroup
	wg.Go(func() {
		for range rounds {
			s.record(live, 1)
			s.record(dropped, 1)
		}
	})
	wg.Go(func() {
		for range 100 {
			var rm metricdata.ResourceMetrics
			_ = s.reader.Collect(t.Context(), &rm)
			s.sweeper.sweep([]string{"live"}, s.now)
		}
	})
	wg.Wait()

	require.Equal(t, int64(rounds), s.flowValues(t, "test_counter")["live"])
}
