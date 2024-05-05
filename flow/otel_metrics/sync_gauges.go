package otel_metrics

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// synchronous gauges are what we want, so we can control when the value is updated
// but they are "experimental", so we resort to using the asynchronous gauges
// but the callback is just a wrapper around the current value, so we can control by calling Set()
type Int64Gauge struct {
	observableGauge metric.Int64ObservableGauge
	observations    map[attribute.Set]*atomic.Int64
}

func NewInt64SyncGauge(meter metric.Meter, gaugeName string, opts ...metric.Int64ObservableGaugeOption) (*Int64Gauge, error) {
	syncGauge := &Int64Gauge{}
	observableGauge, err := meter.Int64ObservableGauge(gaugeName, append(opts, metric.WithInt64Callback(syncGauge.callback))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Int64SyncGauge: %w", err)
	}
	syncGauge.observableGauge = observableGauge
	syncGauge.observations = make(map[attribute.Set]*atomic.Int64)
	return syncGauge, nil
}

func (g *Int64Gauge) callback(ctx context.Context, o metric.Int64Observer) error {
	for attrs, val := range g.observations {
		o.Observe(val.Load(), metric.WithAttributeSet(attrs))
	}
	return nil
}

func (g *Int64Gauge) Set(input int64, attrs attribute.Set) {
	if g == nil {
		return
	}
	val, ok := g.observations[attrs]
	if !ok {
		val = &atomic.Int64{}
		g.observations[attrs] = val
	}
	val.Store(input)
}

type Float64Gauge struct {
	observableGauge      metric.Float64ObservableGauge
	observationsAsUint64 map[attribute.Set]*atomic.Uint64
}

func NewFloat64SyncGauge(meter metric.Meter, gaugeName string, opts ...metric.Float64ObservableGaugeOption) (*Float64Gauge, error) {
	syncGauge := &Float64Gauge{}
	observableGauge, err := meter.Float64ObservableGauge(gaugeName, append(opts, metric.WithFloat64Callback(syncGauge.callback))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Int64SyncGauge: %w", err)
	}
	syncGauge.observableGauge = observableGauge
	syncGauge.observationsAsUint64 = make(map[attribute.Set]*atomic.Uint64)
	return syncGauge, nil
}

func (g *Float64Gauge) callback(ctx context.Context, o metric.Float64Observer) error {
	for attrs, val := range g.observationsAsUint64 {
		o.Observe(math.Float64frombits(val.Load()), metric.WithAttributeSet(attrs))
	}
	return nil
}

func (g *Float64Gauge) Set(input float64, attrs attribute.Set) {
	if g == nil {
		return
	}
	val, ok := g.observationsAsUint64[attrs]
	if !ok {
		val = &atomic.Uint64{}
		g.observationsAsUint64[attrs] = val
	}
	val.Store(math.Float64bits(input))
}

func GetOrInitInt64Gauge(meter metric.Meter, cache map[string]*Int64Gauge,
	name string, opts ...metric.Int64ObservableGaugeOption,
) (*Int64Gauge, error) {
	gauge, ok := cache[name]
	if !ok {
		var err error
		gauge, err = NewInt64SyncGauge(meter, name, opts...)
		if err != nil {
			return nil, err
		}
		cache[name] = gauge
	}
	return gauge, nil
}

func GetOrInitFloat64Gauge(meter metric.Meter, cache map[string]*Float64Gauge,
	name string, opts ...metric.Float64ObservableGaugeOption,
) (*Float64Gauge, error) {
	gauge, ok := cache[name]
	if !ok {
		var err error
		gauge, err = NewFloat64SyncGauge(meter, name, opts...)
		if err != nil {
			return nil, err
		}
		cache[name] = gauge
	}
	return gauge, nil
}
