package otel_metrics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
)

type droppedFlowsInstruments struct {
	counter  metric.Int64Counter
	gauge    metric.Int64Gauge
	evicters []droppedFlowsEvicter
}

func newDroppedFlowsInstruments(t *testing.T, meter metric.Meter) *droppedFlowsInstruments {
	t.Helper()
	counter, err := NewContextAwareInt64Counter(meter, "test_counter")
	require.NoError(t, err)
	gauge, err := ContextAwareInt64Gauge(meter, "test_gauge")
	require.NoError(t, err)
	floatGauge, err := ContextAwareFloat64Gauge(meter, "test_float_gauge")
	require.NoError(t, err)
	evicters := []droppedFlowsEvicter{
		counter.(droppedFlowsEvicter), gauge.(droppedFlowsEvicter), floatGauge.(droppedFlowsEvicter),
	}
	return &droppedFlowsInstruments{counter: counter, gauge: gauge, evicters: evicters}
}

func (i *droppedFlowsInstruments) record(ctx context.Context, value int64) {
	i.counter.Add(ctx, value)
	i.gauge.Record(ctx, value)
}

func flowNamesOf(t *testing.T, points []int64Point) map[string]int64 {
	t.Helper()
	values := make(map[string]int64, len(points))
	for _, point := range points {
		name, _ := point.attrs.Value(FlowNameKey)
		values[name.AsString()] = point.value
	}
	return values
}

func TestEvictDroppedFlowsRemovesSeriesOfDroppedFlows(t *testing.T) {
	meter, reader := newTestMeter(t)
	instruments := newDroppedFlowsInstruments(t, meter)
	instruments.record(flowContext("live"), 1)
	instruments.record(flowContext("dropped"), 1)
	instruments.record(context.Background(), 1)

	evictDroppedFlows(instruments.evicters, []string{"live"}, time.Now().Add(time.Second))

	// The gauge first, since any collection consumes its pending values.
	require.Equal(t, map[string]int64{"live": 1, "": 1}, flowNamesOf(t, collectInt64Points(t, reader, "test_gauge")))
	require.Equal(t, map[string]int64{"live": 1, "": 1}, flowNamesOf(t, collectInt64Points(t, reader, "test_counter")))
}

func TestEvictDroppedFlowsKeepsSeriesCreatedAfterListing(t *testing.T) {
	meter, reader := newTestMeter(t)
	instruments := newDroppedFlowsInstruments(t, meter)
	listedAt := time.Now().Add(-time.Second)
	instruments.record(flowContext("new"), 1)

	evictDroppedFlows(instruments.evicters, nil, listedAt)

	require.Equal(t, map[string]int64{"new": 1}, flowNamesOf(t, collectInt64Points(t, reader, "test_counter")))
}

func TestEvictDroppedFlowsRestartsSeriesRecordedAfterEviction(t *testing.T) {
	meter, reader := newTestMeter(t)
	instruments := newDroppedFlowsInstruments(t, meter)
	// Reusing the context exercises the cached last entry, which must not keep the evicted series alive.
	ctx := flowContext("dropped")
	instruments.record(ctx, 5)

	evictDroppedFlows(instruments.evicters, nil, time.Now().Add(time.Second))
	require.Empty(t, collectInt64Points(t, reader, "test_counter"))

	instruments.record(ctx, 1)
	require.Equal(t, map[string]int64{"dropped": 1}, flowNamesOf(t, collectInt64Points(t, reader, "test_counter")))
}
