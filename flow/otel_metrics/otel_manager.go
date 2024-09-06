package otel_metrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

type OtelManager struct {
	MetricsProvider    *sdkmetric.MeterProvider
	Meter              metric.Meter
	Float64GaugesCache map[string]*Float64SyncGauge
	Int64GaugesCache   map[string]*Int64SyncGauge
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

func setupHttpOtelMetricsExporter() (sdkmetric.Exporter, error) {
	return otlpmetrichttp.New(context.Background())
}

func setupGrpcOtelMetricsExporter() (sdkmetric.Exporter, error) {
	return otlpmetricgrpc.New(context.Background())
}

func SetupOtelMetricsExporter(otelServiceName string) (*sdkmetric.MeterProvider, error) {
	otlpMetricProtocol := peerdbenv.GetEnvString("OTEL_EXPORTER_OTLP_PROTOCOL",
		peerdbenv.GetEnvString("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf"))
	var metricExporter sdkmetric.Exporter
	var err error
	switch otlpMetricProtocol {
	case "http/protobuf":
		metricExporter, err = setupHttpOtelMetricsExporter()
	case "grpc":
		metricExporter, err = setupGrpcOtelMetricsExporter()
	default:
		return nil, fmt.Errorf("unsupported otel metric protocol: %s", otlpMetricProtocol)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry metrics exporter: %w", err)
	}
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(otelResource),
	)
	return meterProvider, nil
}
