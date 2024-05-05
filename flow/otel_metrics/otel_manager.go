package otel_metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

type OtelManager struct {
	MetricsProvider    *sdkmetric.MeterProvider
	Meter              metric.Meter
	Float64GaugesCache map[string]*Float64Gauge
	Int64GaugesCache   map[string]*Int64Gauge
}

// newOtelResource returns a resource describing this application.
func newOtelResource(otelServiceName string) (*resource.Resource, error) {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(otelServiceName),
		),
	)

	return r, err
}

func SetupOtelMetricsExporter() (*sdkmetric.MeterProvider, error) {
	metricExporter, err := otlpmetrichttp.New(context.Background(),
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry metrics exporter: %w", err)
	}

	resource, err := newOtelResource("flow-worker")
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(3*time.Second))),
		sdkmetric.WithResource(resource),
	)
	return meterProvider, nil
}
