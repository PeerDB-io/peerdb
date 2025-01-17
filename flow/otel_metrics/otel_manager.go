package otel_metrics

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/PeerDB-io/peerdb/flow/peerdbenv"
)

const (
	SlotLagGaugeName                    = "cdc_slot_lag"
	CurrentBatchIdGaugeName             = "current_batch_id"
	LastNormalizedBatchIdGaugeName      = "last_normalized_batch_id"
	OpenConnectionsGaugeName            = "open_connections"
	OpenReplicationConnectionsGaugeName = "open_replication_connections"
	IntervalSinceLastNormalizeGaugeName = "interval_since_last_normalize"
	FetchedBytesCounterName             = "fetched_bytes"
	// ErrorEmittedGaugeName This mostly tells whether an error is emitted or not, used for hooking up event based alerting
	ErrorEmittedGaugeName = "error_emitted"
	// ErrorsEmittedCounterName This the actual counter for errors emitted, used for alerting based on error rate/more detailed error analysis
	ErrorsEmittedCounterName = "errors_emitted"
	// RecordsSyncedGaugeName is the gauge name for the number of records synced for every Sync batch
	RecordsSyncedGaugeName = "records_synced"
	// SyncedTablesGaugeName is the gauge name for the number of tables being synced for a mirror
	SyncedTablesGaugeName = "synced_tables"
	// InstanceStatusGaugeName  used for notifying the status of the instance, like if it is healthy/under maintenance etc.
	InstanceStatusGaugeName = "instance_status"
)

type SlotMetricGauges struct {
	SlotLagGauge                    metric.Float64Gauge
	CurrentBatchIdGauge             metric.Int64Gauge
	LastNormalizedBatchIdGauge      metric.Int64Gauge
	OpenConnectionsGauge            metric.Int64Gauge
	OpenReplicationConnectionsGauge metric.Int64Gauge
	IntervalSinceLastNormalizeGauge metric.Float64Gauge
	InstanceStatusGauge             metric.Int64Gauge
}

func BuildMetricName(baseName string) string {
	return peerdbenv.GetPeerDBOtelMetricsNamespace() + baseName
}

type OtelManager struct {
	MetricsProvider    *sdkmetric.MeterProvider
	Meter              metric.Meter
	Float64GaugesCache map[string]metric.Float64Gauge
	Int64GaugesCache   map[string]metric.Int64Gauge
	Int64CountersCache map[string]metric.Int64Counter
}

func NewOtelManager() (*OtelManager, error) {
	metricsProvider, err := SetupPeerDBMetricsProvider("flow-worker")
	if err != nil {
		return nil, err
	}

	return &OtelManager{
		MetricsProvider:    metricsProvider,
		Meter:              metricsProvider.Meter("io.peerdb.flow-worker"),
		Float64GaugesCache: make(map[string]metric.Float64Gauge),
		Int64GaugesCache:   make(map[string]metric.Int64Gauge),
		Int64CountersCache: make(map[string]metric.Int64Counter),
	}, nil
}

func (om *OtelManager) Close(ctx context.Context) error {
	return om.MetricsProvider.Shutdown(ctx)
}

func getOrInitMetric[M any, O any](
	cons func(metric.Meter, string, ...O) (M, error),
	meter metric.Meter,
	cache map[string]M,
	name string,
	opts ...O,
) (M, error) {
	gauge, ok := cache[name]
	if !ok {
		var err error
		gauge, err = cons(meter, name, opts...)
		if err != nil {
			var none M
			return none, err
		}
		cache[name] = gauge
	}
	return gauge, nil
}

func (om *OtelManager) GetOrInitInt64Gauge(name string, opts ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	// Once fixed, replace first argument below with metric.Meter.Int64Gauge
	return getOrInitMetric(Int64Gauge, om.Meter, om.Int64GaugesCache, name, opts...)
}

func (om *OtelManager) GetOrInitFloat64Gauge(name string, opts ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	// Once fixed, replace first argument below with metric.Meter.Float64Gauge
	return getOrInitMetric(Float64Gauge, om.Meter, om.Float64GaugesCache, name, opts...)
}

func (om *OtelManager) GetOrInitInt64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return getOrInitMetric(metric.Meter.Int64Counter, om.Meter, om.Int64CountersCache, name, opts...)
}

// newOtelResource returns a resource describing this application.
func newOtelResource(otelServiceName string, attrs ...attribute.KeyValue) (*resource.Resource, error) {
	allAttrs := append([]attribute.KeyValue{
		semconv.ServiceNameKey.String(otelServiceName),
		attribute.String(DeploymentUidKey, peerdbenv.PeerDBDeploymentUID()),
	}, attrs...)
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			allAttrs...,
		),
	)
}

func temporalMetricsFilteringView() sdkmetric.View {
	exportListString := peerdbenv.GetPeerDBOtelTemporalMetricsExportListEnv()
	slog.Info("Found export list for temporal metrics", slog.String("exportList", exportListString))
	// Special case for exporting all metrics
	if exportListString == "__ALL__" {
		return func(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
			stream := sdkmetric.Stream{
				Name:        BuildMetricName("temporal." + instrument.Name),
				Description: instrument.Description,
				Unit:        instrument.Unit,
			}
			return stream, true
		}
	}
	exportList := strings.Split(exportListString, ",")
	// Don't export any metrics if the list is empty
	if len(exportList) == 0 {
		return func(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
			return sdkmetric.Stream{
				Name:        BuildMetricName("temporal." + instrument.Name),
				Description: instrument.Description,
				Unit:        instrument.Unit,
				Aggregation: sdkmetric.AggregationDrop{},
			}, true
		}
	}

	// Export only the metrics in the list
	enabledMetrics := make(map[string]struct{}, len(exportList))
	for _, metricName := range exportList {
		trimmedMetricName := strings.TrimSpace(metricName)
		enabledMetrics[trimmedMetricName] = struct{}{}
	}
	return func(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
		stream := sdkmetric.Stream{
			Name:        BuildMetricName("temporal." + instrument.Name),
			Description: instrument.Description,
			Unit:        instrument.Unit,
		}
		if _, ok := enabledMetrics[instrument.Name]; !ok {
			stream.Aggregation = sdkmetric.AggregationDrop{}
		}
		return stream, true
	}
}

func setupExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	otlpMetricProtocol := peerdbenv.GetEnvString("OTEL_EXPORTER_OTLP_PROTOCOL",
		peerdbenv.GetEnvString("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf"))
	var metricExporter sdkmetric.Exporter
	var err error
	switch otlpMetricProtocol {
	case "http/protobuf":
		metricExporter, err = otlpmetrichttp.New(ctx)
	case "grpc":
		metricExporter, err = otlpmetricgrpc.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported otel metric protocol: %s", otlpMetricProtocol)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry metrics exporter: %w", err)
	}
	return metricExporter, err
}

func setupMetricsProvider(ctx context.Context, otelResource *resource.Resource, views ...sdkmetric.View) (*sdkmetric.MeterProvider, error) {
	metricExporter, err := setupExporter(ctx)
	if err != nil {
		return nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(otelResource),
		sdkmetric.WithView(views...),
	)
	return meterProvider, nil
}

func SetupPeerDBMetricsProvider(otelServiceName string) (*sdkmetric.MeterProvider, error) {
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}
	return setupMetricsProvider(context.Background(), otelResource)
}

func SetupTemporalMetricsProvider(otelServiceName string) (*sdkmetric.MeterProvider, error) {
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}
	return setupMetricsProvider(context.Background(), otelResource, temporalMetricsFilteringView())
}
