package otel_metrics

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func flowContext(flowName string) context.Context {
	ctx := context.WithValue(context.Background(), internal.FlowMetadataKey, &protos.FlowContextMetadata{
		FlowName: flowName,
		Source: &protos.PeerContextMetadata{
			Name: "src", Type: protos.DBType_MONGO, Hostname: "src-host",
		},
		Destination: &protos.PeerContextMetadata{
			Name: "dst", Type: protos.DBType_CLICKHOUSE, Hostname: "dst-host",
		},
		Status: protos.FlowStatus_STATUS_RUNNING,
	})
	return internal.WithOperationContext(ctx, protos.FlowOperation_FLOW_OPERATION_SYNC)
}

func attrValue(t *testing.T, attrs attribute.Set, key string) string {
	t.Helper()
	value, ok := attrs.Value(attribute.Key(key))
	require.True(t, ok, "missing attribute %s", key)
	return value.String()
}

func newTestMeter(tb testing.TB) (metric.Meter, *sdkmetric.ManualReader) {
	tb.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tb.Cleanup(func() { require.NoError(tb, provider.Shutdown(context.Background())) })
	return provider.Meter("test"), reader
}

type int64Point struct {
	value int64
	attrs attribute.Set
}

// collectInt64Points collects one round and returns the int64 sum and gauge points of the named metric.
func collectInt64Points(t *testing.T, reader *sdkmetric.ManualReader, name string) []int64Point {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var points []int64Point
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				require.True(t, data.IsMonotonic)
				for _, point := range data.DataPoints {
					points = append(points, int64Point{value: point.Value, attrs: point.Attributes})
				}
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					points = append(points, int64Point{value: point.Value, attrs: point.Attributes})
				}
			default:
				t.Fatalf("unexpected data type %T", m.Data)
			}
		}
	}
	return points
}

func TestContextAwareInt64GaugeRecordsContextualAttributes(t *testing.T) {
	meter, reader := newTestMeter(t)
	gauge, err := ContextAwareInt64Gauge(meter, "test_gauge")
	require.NoError(t, err)

	gauge.Record(flowContext("test-flow"), 41)
	gauge.Record(flowContext("test-flow"), 42)

	points := collectInt64Points(t, reader, "test_gauge")
	require.Len(t, points, 1)
	require.Equal(t, int64(42), points[0].value)
	require.Equal(t, "test-flow", attrValue(t, points[0].attrs, FlowNameKey))
	require.Equal(t, "src", attrValue(t, points[0].attrs, SourcePeerName))
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_SYNC.String(), attrValue(t, points[0].attrs, FlowOperationKey))
}

func TestContextAwareGaugeReportsOnlyNewValues(t *testing.T) {
	meter, reader := newTestMeter(t)
	gauge, err := ContextAwareInt64Gauge(meter, "test_gauge")
	require.NoError(t, err)
	ctx := flowContext("test-flow")

	gauge.Record(ctx, 1)
	require.Len(t, collectInt64Points(t, reader, "test_gauge"), 1)
	require.Empty(t, collectInt64Points(t, reader, "test_gauge"))

	gauge.Record(ctx, 0)
	points := collectInt64Points(t, reader, "test_gauge")
	require.Len(t, points, 1)
	require.Zero(t, points[0].value)
}

func TestContextAwareFloat64Gauge(t *testing.T) {
	meter, reader := newTestMeter(t)
	gauge, err := ContextAwareFloat64Gauge(meter, "test_gauge")
	require.NoError(t, err)

	gauge.Record(flowContext("test-flow"), 1.5)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	data, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Len(t, data.DataPoints, 1)
	require.InDelta(t, 1.5, data.DataPoints[0].Value, 0)
}

func TestContextAwareInt64Counter(t *testing.T) {
	meter, reader := newTestMeter(t)
	counter, err := NewContextAwareInt64Counter(meter, "test_counter", metric.WithUnit("By"))
	require.NoError(t, err)
	extra := metric.WithAttributeSet(attribute.NewSet(attribute.String("charset", "latin1")))

	counter.Add(flowContext("flow-a"), 2)
	counter.Add(flowContext("flow-a"), 3)
	counter.Add(flowContext("flow-b"), 1)
	counter.Add(flowContext("flow-a"), 5, extra)

	sums := map[string]int64{}
	for _, point := range collectInt64Points(t, reader, "test_counter") {
		key := attrValue(t, point.attrs, FlowNameKey)
		if charset, ok := point.attrs.Value("charset"); ok {
			key += "/" + charset.AsString()
		}
		sums[key] = point.value
	}
	require.Equal(t, map[string]int64{"flow-a": 5, "flow-b": 1, "flow-a/latin1": 5}, sums)

	counter.Add(flowContext("flow-b"), 4)
	for _, point := range collectInt64Points(t, reader, "test_counter") {
		if attrValue(t, point.attrs, FlowNameKey) == "flow-b" {
			require.Equal(t, int64(5), point.value)
		}
	}
}

func TestContextAwareCounterExtraAttributesOverrideContextual(t *testing.T) {
	meter, reader := newTestMeter(t)
	counter, err := NewContextAwareInt64Counter(meter, "test_counter")
	require.NoError(t, err)

	counter.Add(flowContext("flow-a"), 1, metric.WithAttributeSet(attribute.NewSet(attribute.String(FlowNameKey, "override"))))

	points := collectInt64Points(t, reader, "test_counter")
	require.Len(t, points, 1)
	require.Equal(t, "override", attrValue(t, points[0].attrs, FlowNameKey))
}

func TestContextAwareCounterSharesSeriesAcrossContexts(t *testing.T) {
	meter, reader := newTestMeter(t)
	counter, err := NewContextAwareInt64Counter(meter, "test_counter")
	require.NoError(t, err)

	// a new batch brings a new context with the same attributes; its counts continue the same series
	counter.Add(flowContext("test-flow"), 1)
	counter.Add(flowContext("test-flow"), 1)

	points := collectInt64Points(t, reader, "test_counter")
	require.Len(t, points, 1)
	require.Equal(t, int64(2), points[0].value)
}

func TestContextAwareInt64Histogram(t *testing.T) {
	meter, reader := newTestMeter(t)
	histogram, err := NewContextAwareInt64Histogram(
		meter,
		"test_event_size",
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(100, 200, 500),
	)
	require.NoError(t, err)
	histogram.Record(flowContext("test-flow"), 200)
	histogram.Record(flowContext("test-flow"), 300, metric.WithAttributeSet(attribute.NewSet(attribute.String("kind", "x"))))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)

	got := rm.ScopeMetrics[0].Metrics[0]
	require.Equal(t, "test_event_size", got.Name)
	require.Equal(t, "By", got.Unit)
	data, ok := got.Data.(metricdata.Histogram[int64])
	require.True(t, ok)
	require.Len(t, data.DataPoints, 2)
	for _, point := range data.DataPoints {
		require.Equal(t, []float64{100, 200, 500}, point.Bounds)
		require.Equal(t, uint64(1), point.Count)
		require.Equal(t, "test-flow", attrValue(t, point.Attributes, FlowNameKey))
	}
}

// Writers use distinct ctx objects of the same flow (forcing last/entries eviction) while collection runs.
// Run with -race. No increment may be lost, however `last`/`entries` and `base`/`extra` race.
func TestContextAwareCounterConcurrentAddAndCollect(t *testing.T) {
	meter, reader := newTestMeter(t)
	counter, err := NewContextAwareInt64Counter(meter, "test_counter")
	require.NoError(t, err)
	extra := metric.WithAttributeSet(attribute.NewSet(attribute.String("charset", "latin1")))

	const writers, adds = 8, 20000
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			ctx := flowContext("test-flow") // own ctx per goroutine, same flow name
			for i := range adds {
				if i%4 == 0 {
					counter.Add(ctx, 1, extra)
				} else {
					counter.Add(ctx, 1)
				}
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	var last int64
collecting:
	for {
		select {
		case <-done:
			break collecting
		default:
			var total int64
			for _, point := range collectInt64Points(t, reader, "test_counter") {
				total += point.value
			}
			require.GreaterOrEqual(t, total, last) // cumulative sum never goes backwards
			last = total
		}
	}

	sums := map[bool]int64{}
	for _, point := range collectInt64Points(t, reader, "test_counter") {
		_, hasCharset := point.attrs.Value("charset")
		sums[hasCharset] += point.value
	}
	require.Equal(t, map[bool]int64{true: writers * adds / 4, false: writers * adds * 3 / 4}, sums)
}

// One writer records an increasing sequence while collection runs. Collected values must not go backwards,
// and the last recorded value must be reported by the next collection (the pending flag must not be lost).
func TestContextAwareGaugeConcurrentRecordAndCollect(t *testing.T) {
	meter, reader := newTestMeter(t)
	gauge, err := ContextAwareInt64Gauge(meter, "test_gauge")
	require.NoError(t, err)
	ctx := flowContext("test-flow")

	const records = 200000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := int64(1); i <= records; i++ {
			gauge.Record(ctx, i)
		}
	}()

	var last int64
collecting:
	for {
		select {
		case <-done:
			break collecting
		default:
			for _, point := range collectInt64Points(t, reader, "test_gauge") {
				require.GreaterOrEqual(t, point.value, last)
				last = point.value
			}
		}
	}

	points := collectInt64Points(t, reader, "test_gauge")
	if last < records { // the final value was not yet collected
		require.Len(t, points, 1)
		require.Equal(t, int64(records), points[0].value)
	}
	require.Empty(t, collectInt64Points(t, reader, "test_gauge"))
}

func TestNoopMeterReturnsNoopInstruments(t *testing.T) {
	meter := noop.NewMeterProvider().Meter("test")
	counter, err := NewContextAwareInt64Counter(meter, "c")
	require.NoError(t, err)
	require.IsType(t, noop.Int64Counter{}, counter)
	gauge, err := ContextAwareInt64Gauge(meter, "g")
	require.NoError(t, err)
	require.IsType(t, noop.Int64Gauge{}, gauge)
	floatGauge, err := ContextAwareFloat64Gauge(meter, "f")
	require.NoError(t, err)
	require.IsType(t, noop.Float64Gauge{}, floatGauge)
}

func newAttributeSetCache() *contextAttributesCache[attribute.Set] {
	return &contextAttributesCache[attribute.Set]{resolve: func(attrs attribute.Set) *attribute.Set { return &attrs }}
}

func TestContextAttributesCache(t *testing.T) {
	cache := newAttributeSetCache()

	ctxA := flowContext("flow-a")
	ctxB := flowContext("flow-b")
	derivedCtxA := internal.WithOperationContext(ctxA, protos.FlowOperation_FLOW_OPERATION_NORMALIZE)

	// same context twice: identical attributes (served from cache)
	first := cache.get(ctxA, attribute.Set{})
	require.Same(t, first, cache.get(ctxA, attribute.Set{}))
	require.Equal(t, "flow-a", attrValue(t, *first, FlowNameKey))
	entryA, ok := cache.entries.Load("flow-a")
	require.True(t, ok)

	// a different flow's context must not be served stale attributes
	require.Equal(t, "flow-b", attrValue(t, *cache.get(ctxB, attribute.Set{}), FlowNameKey))

	// switching back is served from the flow's cache entry
	require.Same(t, first, cache.get(ctxA, attribute.Set{}))
	entryA2, ok := cache.entries.Load("flow-a")
	require.True(t, ok)
	require.Same(t, entryA, entryA2)

	// a derived context with a different operation is a different context value
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_NORMALIZE.String(),
		attrValue(t, *cache.get(derivedCtxA, attribute.Set{}), FlowOperationKey))

	// the original context is rebuilt correctly
	attrs := cache.get(ctxA, attribute.Set{})
	require.Equal(t, "flow-a", attrValue(t, *attrs, FlowNameKey))
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_SYNC.String(), attrValue(t, *attrs, FlowOperationKey))

	// extra attributes are merged once per context and reused
	extra := attribute.NewSet(attribute.String("charset", "latin1"))
	merged := cache.get(ctxA, extra)
	require.Same(t, merged, cache.get(ctxA, attribute.NewSet(attribute.String("charset", "latin1"))))
	require.Equal(t, "latin1", attrValue(t, *merged, "charset"))
	require.Equal(t, "flow-a", attrValue(t, *merged, FlowNameKey))
}

func TestContextAttributesWithoutFlowName(t *testing.T) {
	cache := newAttributeSetCache()

	_, ok := cache.entries.Load("")
	require.False(t, ok) // cache is empty

	ctx := context.Background()
	attrs := cache.get(ctx, attribute.Set{})
	_, hasFlowName := attrs.Value(FlowNameKey)
	require.False(t, hasFlowName)
	_, ok = cache.entries.Load("")
	require.True(t, ok) // cache is populated

	// same context is served from cache
	require.Same(t, attrs, cache.get(ctx, attribute.Set{}))

	// different context (also without flow name) rebuilds its own attributes
	otherCtx := internal.WithOperationContext(context.Background(), protos.FlowOperation_FLOW_OPERATION_SYNC)
	other := cache.get(otherCtx, attribute.Set{})
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_SYNC.String(), attrValue(t, *other, FlowOperationKey))
	_, hasFlowName = other.Value(FlowNameKey)
	require.False(t, hasFlowName)
}

func BenchmarkContextAwareInstruments(b *testing.B) {
	meter, _ := newTestMeter(b)
	ctx := flowContext("bench-flow")
	charset := metric.WithAttributeSet(attribute.NewSet(
		attribute.String("charset", "utf8mb4"), attribute.String("status", "not_transcoded")))

	gauge, err := ContextAwareInt64Gauge(meter, "bench_gauge")
	require.NoError(b, err)
	b.Run("gauge", func(b *testing.B) {
		for b.Loop() {
			gauge.Record(ctx, 1)
		}
	})
	b.Run("gauge-cache-miss", func(b *testing.B) {
		otherCtx := flowContext("bench-flow")
		i := 0
		for b.Loop() {
			if i%2 == 0 {
				gauge.Record(ctx, 1)
			} else {
				gauge.Record(otherCtx, 1)
			}
			i++
		}
	})
	b.Run("gauge-interleaved-flows", func(b *testing.B) {
		ctxs := []context.Context{flowContext("bench-1"), flowContext("bench-2"), flowContext("bench-3")}
		i := 0
		for b.Loop() {
			gauge.Record(ctxs[i%len(ctxs)], 1)
			i++
		}
	})

	counter, err := NewContextAwareInt64Counter(meter, "bench_counter")
	require.NoError(b, err)
	b.Run("counter", func(b *testing.B) {
		for b.Loop() {
			counter.Add(ctx, 1)
		}
	})
	b.Run("counter-precomputed-extra", func(b *testing.B) {
		for b.Loop() {
			counter.Add(ctx, 1, charset)
		}
	})
	b.Run("counter-extra-built-per-call", func(b *testing.B) {
		for b.Loop() {
			counter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				attribute.String("charset", "utf8mb4"), attribute.String("status", "not_transcoded"))))
		}
	})

	histogram, err := NewContextAwareInt64Histogram(meter, "bench_histogram")
	require.NoError(b, err)
	b.Run("histogram", func(b *testing.B) {
		for b.Loop() {
			histogram.Record(ctx, 1234)
		}
	})
}

func TestContextAwareCounterConcurrentAdds(t *testing.T) {
	reader := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(sdkmetric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	counter, err := NewContextAwareInt64Counter(provider.Meter("test"), "test_counter")
	require.NoError(t, err)

	const workers, adds = 4, 5000
	var exported int64
	collect := func() {
		for _, point := range collectInt64Points(t, reader, "test_counter") {
			exported += point.value
		}
	}

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			ctx := flowContext("test-flow")
			for range adds {
				counter.Add(ctx, 1)
			}
		})
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	// collect while adding, the deltas must still add up exactly
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
			collect()
		}
	}
	collect()

	require.Equal(t, int64(workers*adds), exported)
}
