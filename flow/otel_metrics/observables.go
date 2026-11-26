package otel_metrics

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/metric/noop"
	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// Counters and gauges keep each series (attribute set) in atomics and report to the SDK only on collection.
// Every SDK measurement walks the whole attribute set, too slow for per-event hot paths.
// Series are never removed, same as contextAttributesCache entries.

func buildContextualAttributeSet(ctx context.Context) attribute.Set {
	attributes := make([]attribute.KeyValue, 0, 12)
	flowMetadata := internal.GetFlowMetadata(ctx)
	if flowMetadata != nil {
		attributes = append(attributes, attribute.String(FlowNameKey, flowMetadata.FlowName))
		if flowMetadata.Source != nil {
			attributes = append(attributes,
				attribute.Stringer(SourcePeerType, flowMetadata.Source.Type),
				attribute.Stringer(SourcePeerVariant, flowMetadata.Source.Variant),
				attribute.String(SourcePeerName, flowMetadata.Source.Name),
				attribute.String(sourcePeerHostname, flowMetadata.Source.Hostname))
		}
		if flowMetadata.Destination != nil {
			attributes = append(attributes,
				attribute.Stringer(DestinationPeerType, flowMetadata.Destination.Type),
				attribute.String(DestinationPeerName, flowMetadata.Destination.Name),
				attribute.String(destinationPeerHostname, flowMetadata.Destination.Hostname))
		}
		attributes = append(attributes,
			attribute.Stringer(FlowStatusKey, flowMetadata.Status),
			attribute.Bool(IsFlowResyncKey, flowMetadata.IsResync))
		if pipeName := flowMetadata.Tags[common.PipeNameTag]; pipeName != "" {
			attributes = append(attributes, attribute.String(PipeNameKey, pipeName))
		}
	}

	additionalMetadata := internal.GetAdditionalMetadata(ctx)
	attributes = append(attributes, attribute.Stringer(FlowOperationKey, additionalMetadata.Operation))

	if activity.IsActivity(ctx) {
		activityInfo := activity.GetInfo(ctx)
		attributes = append(attributes,
			attribute.Bool(IsTemporalActivityKey, true),
			attribute.Bool(IsTemporalLocalActivityKey, activityInfo.IsLocalActivity),
			attribute.String(TemporalActivityTypeKey, activityInfo.ActivityType.Name),
			attribute.String(TemporalWorkflowTypeKey, activityInfo.WorkflowType.Name),
		)
	}
	return attribute.NewSet(attributes...)
}

// mergeAttributeSets adds the call's attributes to the contextual ones. The call's value wins on shared keys.
func mergeAttributeSets(contextual attribute.Set, extra attribute.Set) attribute.Set {
	kvs := make([]attribute.KeyValue, 0, contextual.Len()+extra.Len())
	kvs = append(kvs, contextual.ToSlice()...)
	kvs = append(kvs, extra.ToSlice()...)
	return attribute.NewSet(kvs...)
}

// contextAttributesCache memoizes buildContextualAttributeSet, which is expensive in the hot path, and the
// series state derived from it. Instruments are shared across mirrors, so entries are keyed by flow name
// (when present) to keep mirrors from evicting each other. The last used entry is checked first, since hot
// paths record with the same context repeatedly.
type contextAttributesCache[V any] struct {
	resolve func(attrs attribute.Set) *V
	last    atomic.Pointer[contextEntry[V]]
	entries sync.Map // flow name -> *contextEntry[V]
}

type contextEntry[V any] struct {
	//nolint:containedctx // the context is a cache key compared by identity, not a stored dependency
	ctx   context.Context
	attrs attribute.Set
	// Series state for calls without extra attributes.
	base atomic.Pointer[V]
	// Series state for calls with extra attributes: attribute.Distinct of the extra set -> *V.
	extra sync.Map
}

func (c *contextAttributesCache[V]) entry(ctx context.Context) *contextEntry[V] {
	if entry := c.last.Load(); entry != nil && entry.ctx == ctx {
		return entry
	}
	var flowName string
	if flowMetadata := internal.GetFlowMetadata(ctx); flowMetadata != nil {
		flowName = flowMetadata.FlowName
	}
	if cached, ok := c.entries.Load(flowName); ok {
		if entry := cached.(*contextEntry[V]); entry.ctx == ctx {
			c.last.Store(entry)
			return entry
		}
	}
	entry := &contextEntry[V]{ctx: ctx, attrs: buildContextualAttributeSet(ctx)}
	c.entries.Store(flowName, entry)
	c.last.Store(entry)
	return entry
}

// get returns the series state for the context's attributes plus extra.
// Callers should pass precomputed extra sets; building one per call is the cost this cache avoids.
func (c *contextAttributesCache[V]) get(ctx context.Context, extra attribute.Set) *V {
	entry := c.entry(ctx)
	if extra.Len() == 0 {
		if v := entry.base.Load(); v != nil {
			return v
		}
		v := c.resolve(entry.attrs)
		entry.base.Store(v)
		return v
	}
	key := extra.Equivalent()
	if cached, ok := entry.extra.Load(key); ok {
		return cached.(*V)
	}
	v := c.resolve(mergeAttributeSets(entry.attrs, extra))
	entry.extra.Store(key, v)
	return v
}

func loadOrStoreSeries[T any](allSeries *sync.Map, attrs attribute.Set, newSeries func(attribute.Set) *T) *T {
	key := attrs.Equivalent()
	if series, ok := allSeries.Load(key); ok {
		return series.(*T)
	}
	series, _ := allSeries.LoadOrStore(key, newSeries(attrs))
	return series.(*T)
}

type counterSeries struct {
	attrs attribute.Set
	sum   atomic.Int64
}

type ContextAwareInt64Counter struct {
	embedded.Int64Counter
	contexts contextAttributesCache[counterSeries]
	series   sync.Map // attribute.Distinct -> *counterSeries
}

func (c *ContextAwareInt64Counter) Enabled(context.Context) bool {
	return true
}

func (c *ContextAwareInt64Counter) Add(ctx context.Context, value int64, options ...metric.AddOption) {
	c.contexts.get(ctx, metric.NewAddConfig(options).Attributes()).sum.Add(value)
}

func (c *ContextAwareInt64Counter) observe(_ context.Context, observer metric.Int64Observer) error {
	c.series.Range(func(_, value any) bool {
		series := value.(*counterSeries)
		observer.Observe(series.sum.Load(), metric.WithAttributeSet(series.attrs))
		return true
	})
	return nil
}

func NewContextAwareInt64Counter(meter metric.Meter, name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	counterConfig := metric.NewInt64CounterConfig(opts...)
	counter := &ContextAwareInt64Counter{}
	counter.contexts.resolve = func(attrs attribute.Set) *counterSeries {
		return loadOrStoreSeries(&counter.series, attrs, func(attrs attribute.Set) *counterSeries {
			return &counterSeries{attrs: attrs}
		})
	}
	observable, err := meter.Int64ObservableCounter(name,
		metric.WithDescription(counterConfig.Description()),
		metric.WithUnit(counterConfig.Unit()),
		metric.WithInt64Callback(counter.observe),
	)
	if err != nil {
		return nil, err
	}
	if _, isNoop := observable.(noop.Int64ObservableCounter); isNoop {
		// A noop meter never collects, so recorded series would never be reported.
		return noop.Int64Counter{}, nil
	}
	return counter, nil
}

// gaugeSeries keeps the latest value. Collections report it only if recorded since the previous
// collection, so idle series report nothing.
type gaugeSeries struct {
	attrs   attribute.Set
	bits    atomic.Uint64
	pending atomic.Bool
}

type contextAwareGauge struct {
	contexts contextAttributesCache[gaugeSeries]
	series   sync.Map // attribute.Distinct -> *gaugeSeries
}

func (g *contextAwareGauge) init() {
	g.contexts.resolve = func(attrs attribute.Set) *gaugeSeries {
		return loadOrStoreSeries(&g.series, attrs, func(attrs attribute.Set) *gaugeSeries {
			return &gaugeSeries{attrs: attrs}
		})
	}
}

func (g *contextAwareGauge) record(ctx context.Context, bits uint64, extra attribute.Set) {
	series := g.contexts.get(ctx, extra)
	// Store the value before the flag, so a collection seeing the flag reads this value or a newer one.
	series.bits.Store(bits)
	series.pending.Store(true)
}

func (g *contextAwareGauge) collect(observe func(bits uint64, attrs attribute.Set)) {
	g.series.Range(func(_, value any) bool {
		series := value.(*gaugeSeries)
		if series.pending.Swap(false) {
			observe(series.bits.Load(), series.attrs)
		}
		return true
	})
}

type contextAwareInt64Gauge struct {
	embedded.Int64Gauge
	gauge contextAwareGauge
}

func (g *contextAwareInt64Gauge) Enabled(context.Context) bool {
	return true
}

func (g *contextAwareInt64Gauge) Record(ctx context.Context, value int64, options ...metric.RecordOption) {
	g.gauge.record(ctx, uint64(value), metric.NewRecordConfig(options).Attributes())
}

func (g *contextAwareInt64Gauge) observe(_ context.Context, observer metric.Int64Observer) error {
	g.gauge.collect(func(bits uint64, attrs attribute.Set) {
		observer.Observe(int64(bits), metric.WithAttributeSet(attrs))
	})
	return nil
}

func ContextAwareInt64Gauge(meter metric.Meter, name string, opts ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	gaugeConfig := metric.NewInt64GaugeConfig(opts...)
	gauge := &contextAwareInt64Gauge{}
	gauge.gauge.init()
	observable, err := meter.Int64ObservableGauge(name,
		metric.WithDescription(gaugeConfig.Description()),
		metric.WithUnit(gaugeConfig.Unit()),
		metric.WithInt64Callback(gauge.observe),
	)
	if err != nil {
		return nil, err
	}
	if _, isNoop := observable.(noop.Int64ObservableGauge); isNoop {
		return noop.Int64Gauge{}, nil
	}
	return gauge, nil
}

type contextAwareFloat64Gauge struct {
	embedded.Float64Gauge
	gauge contextAwareGauge
}

func (g *contextAwareFloat64Gauge) Enabled(context.Context) bool {
	return true
}

func (g *contextAwareFloat64Gauge) Record(ctx context.Context, value float64, options ...metric.RecordOption) {
	g.gauge.record(ctx, math.Float64bits(value), metric.NewRecordConfig(options).Attributes())
}

func (g *contextAwareFloat64Gauge) observe(_ context.Context, observer metric.Float64Observer) error {
	g.gauge.collect(func(bits uint64, attrs attribute.Set) {
		observer.Observe(math.Float64frombits(bits), metric.WithAttributeSet(attrs))
	})
	return nil
}

func ContextAwareFloat64Gauge(meter metric.Meter, name string, opts ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	gaugeConfig := metric.NewFloat64GaugeConfig(opts...)
	gauge := &contextAwareFloat64Gauge{}
	gauge.gauge.init()
	observable, err := meter.Float64ObservableGauge(name,
		metric.WithDescription(gaugeConfig.Description()),
		metric.WithUnit(gaugeConfig.Unit()),
		metric.WithFloat64Callback(gauge.observe),
	)
	if err != nil {
		return nil, err
	}
	if _, isNoop := observable.(noop.Float64ObservableGauge); isNoop {
		return noop.Float64Gauge{}, nil
	}
	return gauge, nil
}

// Histograms still record into the SDK on every call, since they rely on its bucket aggregation.
type histogramSeries struct {
	options []metric.RecordOption
}

type ContextAwareInt64Histogram struct {
	metric.Int64Histogram
	contexts contextAttributesCache[histogramSeries]
}

func (h *ContextAwareInt64Histogram) Record(ctx context.Context, value int64, options ...metric.RecordOption) {
	series := h.contexts.get(ctx, metric.NewRecordConfig(options).Attributes())
	h.Int64Histogram.Record(ctx, value, series.options...)
}

func NewContextAwareInt64Histogram(
	meter metric.Meter,
	name string,
	opts ...metric.Int64HistogramOption,
) (metric.Int64Histogram, error) {
	histogramConfig := metric.NewInt64HistogramConfig(opts...)
	histogram, err := meter.Int64Histogram(name,
		metric.WithDescription(histogramConfig.Description()),
		metric.WithUnit(histogramConfig.Unit()),
		metric.WithExplicitBucketBoundaries(histogramConfig.ExplicitBucketBoundaries()...),
	)
	if err != nil {
		return nil, err
	}

	contextAware := &ContextAwareInt64Histogram{Int64Histogram: histogram}
	contextAware.contexts.resolve = func(attrs attribute.Set) *histogramSeries {
		return &histogramSeries{options: []metric.RecordOption{metric.WithAttributeSet(attrs)}}
	}
	return contextAware, nil
}
