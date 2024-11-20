package otel_metrics

import (
	"go.opentelemetry.io/otel/metric"
)

func GetOrInitInt64SyncGauge(meter metric.Meter, cache map[string]metric.Int64Gauge, name string, opts ...metric.Int64GaugeOption,
) (metric.Int64Gauge, error) {
	gauge, ok := cache[name]
	if !ok {
		var err error
		gauge, err = meter.Int64Gauge(name, opts...)
		if err != nil {
			return nil, err
		}
		cache[name] = gauge
	}
	return gauge, nil
}

func GetOrInitFloat64SyncGauge(meter metric.Meter, cache map[string]metric.Float64Gauge, name string, opts ...metric.Float64GaugeOption,
) (metric.Float64Gauge, error) {
	gauge, ok := cache[name]
	if !ok {
		var err error
		gauge, err = meter.Float64Gauge(name, opts...)
		if err != nil {
			return nil, err
		}
		cache[name] = gauge
	}
	return gauge, nil
}
