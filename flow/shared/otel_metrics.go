package shared

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"
)

// synchronous gauges are what we want, so we can control when the value is updated
// but they are "experimental", so we resort to using the asynchronous gauges
// but the callback is just a wrapper around the current value, so we can control by calling Set()
type Int64Gauge struct {
	observableGauge metric.Int64ObservableGauge
	currentVal      atomic.Int64
}

func NewInt64SyncGauge(meter metric.Meter, gaugeName string, opts ...metric.Int64ObservableGaugeOption) (*Int64Gauge, error) {
	syncGauge := &Int64Gauge{}
	observableGauge, err := meter.Int64ObservableGauge(gaugeName, append(opts, metric.WithInt64Callback(syncGauge.callback))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Int64SyncGauge: %w", err)
	}
	syncGauge.observableGauge = observableGauge
	return syncGauge, nil
}

func (g *Int64Gauge) callback(ctx context.Context, o metric.Int64Observer) error {
	o.Observe(g.currentVal.Load())
	return nil
}

func (g *Int64Gauge) Set(val int64) {
	if g == nil {
		return
	}
	g.currentVal.Store(val)
}

type Float64Gauge struct {
	observableGauge metric.Float64ObservableGauge
	currentValAsU64 atomic.Uint64
}

func NewFloat64SyncGauge(meter metric.Meter, gaugeName string, opts ...metric.Float64ObservableGaugeOption) (*Float64Gauge, error) {
	syncGauge := &Float64Gauge{}
	observableGauge, err := meter.Float64ObservableGauge(gaugeName, append(opts, metric.WithFloat64Callback(syncGauge.callback))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Int64SyncGauge: %w", err)
	}
	syncGauge.observableGauge = observableGauge
	return syncGauge, nil
}

func (g *Float64Gauge) callback(ctx context.Context, o metric.Float64Observer) error {
	o.Observe(math.Float64frombits(g.currentValAsU64.Load()))
	return nil
}

func (g *Float64Gauge) Set(val float64) {
	if g == nil {
		return
	}
	g.currentValAsU64.Store(math.Float64bits(val))
}
