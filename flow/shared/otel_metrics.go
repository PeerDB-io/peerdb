package shared

import (
	"context"
	"fmt"
	"sync"
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
	observableGauge, err := meter.Int64ObservableGauge(gaugeName, append(opts, metric.WithInt64Callback(syncGauge.Callback))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Int64SyncGauge: %w", err)
	}
	syncGauge.observableGauge = observableGauge
	return syncGauge, nil
}

func (g *Int64Gauge) Callback(ctx context.Context, o metric.Int64Observer) error {
	o.Observe(g.currentVal.Load())
	return nil
}

func (g *Int64Gauge) Set(val int64) {
	g.currentVal.Store(val)
}

type Float64Gauge struct {
	observableGauge metric.Float64ObservableGauge
	floatMutex      sync.Mutex
	currentVal      float64
}

func NewFloat64SyncGauge(meter metric.Meter, gaugeName string, opts ...metric.Float64ObservableGaugeOption) (*Float64Gauge, error) {
	syncGauge := &Float64Gauge{}
	observableGauge, err := meter.Float64ObservableGauge(gaugeName, append(opts, metric.WithFloat64Callback(syncGauge.Callback))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Int64SyncGauge: %w", err)
	}
	syncGauge.observableGauge = observableGauge
	return syncGauge, nil
}

func (g *Float64Gauge) Callback(ctx context.Context, o metric.Float64Observer) error {
	g.floatMutex.Lock()
	defer g.floatMutex.Unlock()
	o.Observe(g.currentVal)
	return nil
}

func (g *Float64Gauge) Set(val float64) {
	g.floatMutex.Lock()
	defer g.floatMutex.Unlock()
	g.currentVal = val
}
