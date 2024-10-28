package otel_metrics

import (
	"context"
	"fmt"
	"log"
	"strings"

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

func temporalMetricsFilteringView() sdkmetric.View {
	exportListString := peerdbenv.GetEnvString("PEERDB_TEMPORAL_OTEL_METRICS_EXPORT_LIST", "")
	// TODO remove below log
	log.Printf("exportListString: %s", exportListString)
	// Special case for exporting all metrics
	if exportListString == "__ALL__" {
		return func(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
			return sdkmetric.Stream{
				Name:            GetPeerDBOtelMetricsNamespace() + "temporal." + instrument.Name,
				Description:     instrument.Description,
				Unit:            instrument.Unit,
				Aggregation:     nil,
				AttributeFilter: nil,
			}, true
		}
	}
	exportList := strings.Split(exportListString, ",")
	// Don't export any metrics if the list is empty
	if len(exportList) == 0 {
		return func(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
			return sdkmetric.Stream{}, false
		}
	}

	// Export only the metrics in the list
	enabledMetrics := make(map[string]struct{})
	for _, metricName := range exportList {
		enabledMetrics[metricName] = struct{}{}
	}
	return func(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
		if _, ok := enabledMetrics[instrument.Name]; ok {
			return sdkmetric.Stream{
				Name:            GetPeerDBOtelMetricsNamespace() + "temporal." + instrument.Name,
				Description:     instrument.Description,
				Unit:            instrument.Unit,
				Aggregation:     nil,
				AttributeFilter: nil,
			}, true
		}
		return sdkmetric.Stream{}, false
	}
}

func setupExporter() (sdkmetric.Exporter, error) {
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
	return metricExporter, err
}

func setupMetricsProvider(otelServiceName string, views ...sdkmetric.View) (*sdkmetric.MeterProvider, error) {
	metricExporter, err := setupExporter()
	if err != nil {
		return nil, err
	}
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(otelResource),
		sdkmetric.WithView(views...),
	)
	return meterProvider, nil
}

func SetupPeerDBMetricsProvider(otelServiceName string) (*sdkmetric.MeterProvider, error) {
	return setupMetricsProvider(otelServiceName)
}

func SetupTemporalMetricsProvider(otelServiceName string) (*sdkmetric.MeterProvider, error) {
	return setupMetricsProvider(otelServiceName, temporalMetricsFilteringView())
}
