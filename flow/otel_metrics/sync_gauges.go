package otel_metrics

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ObservationMapValue[V comparable] struct {
	Value V
}

// SyncGauge is a generic synchronous gauge that can be used to observe any type of value
// Inspired from https://github.com/open-telemetry/opentelemetry-go/issues/3984#issuecomment-1743231837
type SyncGauge[V comparable, O metric.Observable] struct {
	observableGauge O
	observations    sync.Map
	name            string
}

func (a *SyncGauge[V, O]) Callback(ctx context.Context, observeFunc func(value V, options ...metric.ObserveOption)) error {
	a.observations.Range(func(key, value interface{}) bool {
		attrs := key.(attribute.Set)
		val := value.(*ObservationMapValue[V])
		observeFunc(val.Value, metric.WithAttributeSet(attrs))
		// If the pointer is still same we can safely delete, else it means that the value was overwritten in parallel
		a.observations.CompareAndDelete(attrs, val)
		return true
	})
	return nil
}

func (a *SyncGauge[V, O]) Set(input V, attrs attribute.Set) {
	val := ObservationMapValue[V]{Value: input}
	a.observations.Store(attrs, &val)
}

type Int64SyncGauge struct {
	syncGauge *SyncGauge[int64, metric.Int64Observable]
}

func (a *Int64SyncGauge) Set(input int64, attrs attribute.Set) {
	if a == nil {
		return
	}
	a.syncGauge.Set(input, attrs)
}

func NewInt64SyncGauge(meter metric.Meter, gaugeName string, opts ...metric.Int64ObservableGaugeOption) (*Int64SyncGauge, error) {
	syncGauge := &SyncGauge[int64, metric.Int64Observable]{
		name: gaugeName,
	}
	observableGauge, err := meter.Int64ObservableGauge(gaugeName,
		append(opts, metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			return syncGauge.Callback(ctx, func(value int64, options ...metric.ObserveOption) {
				observer.Observe(value, options...)
			})
		}))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Int64SyncGauge: %w", err)
	}
	syncGauge.observableGauge = observableGauge
	return &Int64SyncGauge{syncGauge: syncGauge}, nil
}

type Float64SyncGauge struct {
	syncGauge *SyncGauge[float64, metric.Float64Observable]
}

func (a *Float64SyncGauge) Set(input float64, attrs attribute.Set) {
	if a == nil {
		return
	}
	a.syncGauge.Set(input, attrs)
}

func NewFloat64SyncGauge(meter metric.Meter, gaugeName string, opts ...metric.Float64ObservableGaugeOption) (*Float64SyncGauge, error) {
	syncGauge := &SyncGauge[float64, metric.Float64Observable]{
		name: gaugeName,
	}
	observableGauge, err := meter.Float64ObservableGauge(gaugeName,
		append(opts, metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
			return syncGauge.Callback(ctx, func(value float64, options ...metric.ObserveOption) {
				observer.Observe(value, options...)
			})
		}))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Float64SyncGauge: %w", err)
	}
	syncGauge.observableGauge = observableGauge
	return &Float64SyncGauge{syncGauge: syncGauge}, nil
}

func GetOrInitInt64SyncGauge(meter metric.Meter, cache map[string]*Int64SyncGauge, name string,
	opts ...metric.Int64ObservableGaugeOption,
) (*Int64SyncGauge, error) {
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

func GetOrInitFloat64SyncGauge(meter metric.Meter, cache map[string]*Float64SyncGauge,
	name string, opts ...metric.Float64ObservableGaugeOption,
) (*Float64SyncGauge, error) {
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
