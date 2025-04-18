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
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

const (
	FlowWorkerServiceName         = "flow-worker"
	FlowSnapshotWorkerServiceName = "flow-snapshot-worker"
	FlowApiServiceName            = "flow-api"
)

const (
	SlotLagGaugeName                    = "cdc_slot_lag"
	CurrentBatchIdGaugeName             = "current_batch_id"
	LastNormalizedBatchIdGaugeName      = "last_normalized_batch_id"
	OpenConnectionsGaugeName            = "open_connections"
	OpenReplicationConnectionsGaugeName = "open_replication_connections"
	CommittedLSNGaugeName               = "committed_lsn"
	RestartLSNGaugeName                 = "restart_lsn"
	ConfirmedFlushLSNGaugeName          = "confirmed_flush_lsn"
	IntervalSinceLastNormalizeGaugeName = "interval_since_last_normalize"
	FetchedBytesCounterName             = "fetched_bytes"
	ErrorEmittedGaugeName               = "error_emitted"
	ErrorsEmittedCounterName            = "errors_emitted"
	RecordsSyncedGaugeName              = "records_synced"
	RecordsSyncedCounterName            = "records_synced_counter"
	SyncedTablesGaugeName               = "synced_tables"
	InstanceStatusGaugeName             = "instance_status"
	MaintenanceStatusGaugeName          = "maintenance_status"
	FlowStatusGaugeName                 = "flow_status"
	ActiveFlowsGaugeName                = "active_flows"
)

type Metrics struct {
	SlotLagGauge                    metric.Float64Gauge
	CurrentBatchIdGauge             metric.Int64Gauge
	LastNormalizedBatchIdGauge      metric.Int64Gauge
	OpenConnectionsGauge            metric.Int64Gauge
	OpenReplicationConnectionsGauge metric.Int64Gauge
	CommittedLSNGauge               metric.Int64Gauge
	RestartLSNGauge                 metric.Int64Gauge
	ConfirmedFlushLSNGauge          metric.Int64Gauge
	IntervalSinceLastNormalizeGauge metric.Float64Gauge
	FetchedBytesCounter             metric.Int64Counter
	ErrorEmittedGauge               metric.Int64Gauge
	ErrorsEmittedCounter            metric.Int64Counter
	RecordsSyncedGauge              metric.Int64Gauge
	RecordsSyncedCounter            metric.Int64Counter
	SyncedTablesGauge               metric.Int64Gauge
	InstanceStatusGauge             metric.Int64Gauge
	MaintenanceStatusGauge          metric.Int64Gauge
	FlowStatusGauge                 metric.Int64Gauge
	ActiveFlowsGauge                metric.Int64Gauge
	CPULimitsPerActiveFlowGauge     metric.Float64Gauge
	MemoryLimitsPerActiveFlowGauge  metric.Float64Gauge
}

type SlotMetricGauges struct {
	SlotLagGauge                    metric.Float64Gauge
	RestartLSNGauge                 metric.Int64Gauge
	ConfirmedFlushLSNGauge          metric.Int64Gauge
	CurrentBatchIdGauge             metric.Int64Gauge
	LastNormalizedBatchIdGauge      metric.Int64Gauge
	OpenConnectionsGauge            metric.Int64Gauge
	OpenReplicationConnectionsGauge metric.Int64Gauge
	IntervalSinceLastNormalizeGauge metric.Float64Gauge
	InstanceStatusGauge             metric.Int64Gauge
}

func BuildMetricName(baseName string) string {
	return internal.GetPeerDBOtelMetricsNamespace() + baseName
}

type OtelManager struct {
	Metrics            Metrics
	MetricsProvider    metric.MeterProvider
	Meter              metric.Meter
	Float64GaugesCache map[string]metric.Float64Gauge
	Int64GaugesCache   map[string]metric.Int64Gauge
	Int64CountersCache map[string]metric.Int64Counter
	Enabled            bool
}

func NewOtelManager(ctx context.Context, serviceName string, enabled bool) (*OtelManager, error) {
	metricsProvider, err := SetupPeerDBMetricsProvider(ctx, serviceName, enabled)
	if err != nil {
		return nil, err
	}

	otelManager := OtelManager{
		Enabled:            enabled,
		MetricsProvider:    metricsProvider,
		Meter:              metricsProvider.Meter("io.peerdb." + serviceName),
		Float64GaugesCache: make(map[string]metric.Float64Gauge),
		Int64GaugesCache:   make(map[string]metric.Int64Gauge),
		Int64CountersCache: make(map[string]metric.Int64Counter),
	}
	if err := otelManager.setupMetrics(); err != nil {
		return nil, err
	}
	return &otelManager, nil
}

func (om *OtelManager) Close(ctx context.Context) error {
	if provider, ok := om.MetricsProvider.(*sdkmetric.MeterProvider); ok {
		return provider.Shutdown(ctx)
	}
	return nil
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
	return getOrInitMetric(ContextAwareInt64Gauge, om.Meter, om.Int64GaugesCache, name, opts...)
}

func (om *OtelManager) GetOrInitFloat64Gauge(name string, opts ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	// Once fixed, replace first argument below with metric.Meter.Float64Gauge
	return getOrInitMetric(ContextAwareFloat64Gauge, om.Meter, om.Float64GaugesCache, name, opts...)
}

func (om *OtelManager) GetOrInitInt64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return getOrInitMetric(NewContextAwareInt64Counter, om.Meter, om.Int64CountersCache, name, opts...)
}

func (om *OtelManager) setupMetrics() error {
	slog.Debug("Setting up all metrics")
	var err error
	if om.Metrics.SlotLagGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(SlotLagGaugeName),
		metric.WithUnit("MiBy"),
		metric.WithDescription("Postgres replication slot lag in MB"),
	); err != nil {
		return err
	}

	if om.Metrics.CurrentBatchIdGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(CurrentBatchIdGaugeName)); err != nil {
		return err
	}

	if om.Metrics.LastNormalizedBatchIdGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(LastNormalizedBatchIdGaugeName)); err != nil {
		return err
	}

	if om.Metrics.OpenConnectionsGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(OpenConnectionsGaugeName),
		metric.WithDescription("Current open connections for PeerDB user"),
	); err != nil {
		return err
	}

	if om.Metrics.OpenReplicationConnectionsGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(OpenReplicationConnectionsGaugeName),
		metric.WithDescription("Current open replication connections for PeerDB user"),
	); err != nil {
		return err
	}

	if om.Metrics.CommittedLSNGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(CommittedLSNGaugeName),
		metric.WithDescription("Committed LSN of the replication slot"),
	); err != nil {
		return err
	}

	if om.Metrics.RestartLSNGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(RestartLSNGaugeName),
		metric.WithDescription("Restart LSN of the replication slot"),
	); err != nil {
		return err
	}

	if om.Metrics.ConfirmedFlushLSNGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(ConfirmedFlushLSNGaugeName),
		metric.WithDescription("Confirmed flush LSN of the replication slot"),
	); err != nil {
		return err
	}

	if om.Metrics.IntervalSinceLastNormalizeGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(IntervalSinceLastNormalizeGaugeName),
		metric.WithUnit("s"),
		metric.WithDescription("Interval since last normalize"),
	); err != nil {
		return err
	}

	if om.Metrics.FetchedBytesCounter, err = om.GetOrInitInt64Counter(BuildMetricName(FetchedBytesCounterName),
		metric.WithUnit("By"),
		metric.WithDescription("Bytes received of CopyData over replication slot"),
	); err != nil {
		return err
	}

	if om.Metrics.ErrorEmittedGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(ErrorEmittedGaugeName),
		// This mostly tells whether an error is emitted or not, used for hooking up event based alerting
		metric.WithDescription("Whether an error was emitted, 1 if emitted, 0 otherwise"),
	); err != nil {
		return err
	}

	if om.Metrics.ErrorsEmittedCounter, err = om.GetOrInitInt64Counter(BuildMetricName(ErrorsEmittedCounterName),
		// This the actual counter for errors emitted, used for alerting based on error rate/more detailed error analysis
		metric.WithDescription("Counter of errors emitted"),
	); err != nil {
		return err
	}

	if om.Metrics.RecordsSyncedGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(RecordsSyncedGaugeName),
		metric.WithDescription("Number of records synced for every Sync batch"),
	); err != nil {
		return err
	}

	if om.Metrics.RecordsSyncedCounter, err = om.GetOrInitInt64Counter(BuildMetricName(RecordsSyncedCounterName),
		metric.WithDescription("Counter of records synced (all time)"),
	); err != nil {
		return err
	}

	if om.Metrics.SyncedTablesGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(SyncedTablesGaugeName),
		metric.WithDescription("Number of tables synced"),
	); err != nil {
		return err
	}

	if om.Metrics.InstanceStatusGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(InstanceStatusGaugeName),
		metric.WithDescription("Status of the instance, always emits a 1 metric with different attributes for different statuses"),
	); err != nil {
		return err
	}

	if om.Metrics.MaintenanceStatusGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(MaintenanceStatusGaugeName),
		metric.WithDescription("Whether maintenance is running, 1 if running with different attributes for start/end"),
	); err != nil {
		return err
	}
	slog.Debug("Finished setting up all metrics")

	if om.Metrics.FlowStatusGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(FlowStatusGaugeName),
		metric.WithDescription("Status of the flow, always emits a 1 metric with different `flowStatus` value for different statuses"),
	); err != nil {
		return err
	}

	if om.Metrics.ActiveFlowsGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(ActiveFlowsGaugeName),
		metric.WithDescription("Number of active flows"),
	); err != nil {
		return err
	}

	// Appending unit since UCUM does not support `vcores` as a unit
	if om.Metrics.CPULimitsPerActiveFlowGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName("cpu_limits_per_active_flow_vcores"),
		metric.WithDescription(
			"CPU limits per active flow. To get total CPU limits, multiply by number of active flows or do sum over all flows",
		),
	); err != nil {
		return err
	}

	if om.Metrics.MemoryLimitsPerActiveFlowGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName("memory_limits_per_active_flow"),
		metric.WithDescription(
			"Memory per active flow. To get total memory limits, multiply by number of active flows or do sum over all flows",
		),
		metric.WithUnit("By"),
	); err != nil {
		return err
	}

	return nil
}

// newOtelResource returns a resource describing this application.
func newOtelResource(otelServiceName string, attrs ...attribute.KeyValue) (*resource.Resource, error) {
	allAttrs := append([]attribute.KeyValue{
		semconv.ServiceNameKey.String(otelServiceName),
		attribute.String(DeploymentUidKey, internal.PeerDBDeploymentUID()),
		semconv.ServiceVersion(internal.PeerDBVersionShaShort()),
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
	exportListString := internal.GetPeerDBOtelTemporalMetricsExportListEnv()
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

// componentMetricsRenamingView renames the metrics to include the component name and any prefix
func componentMetricsRenamingView(componentName string) sdkmetric.View {
	return func(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
		stream := sdkmetric.Stream{
			Name:        BuildMetricName(componentName + "." + instrument.Name),
			Description: instrument.Description,
			Unit:        instrument.Unit,
		}
		return stream, true
	}
}

type panicOnFailureExporter struct {
	sdkmetric.Exporter
}

func (p *panicOnFailureExporter) Export(ctx context.Context, metrics *metricdata.ResourceMetrics) error {
	if err := p.Exporter.Export(ctx, metrics); err != nil {
		panic(fmt.Sprintf("[panicOnFailureExporter] failed to export metrics: %v", err))
	}
	return nil
}

func setupExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	otlpMetricProtocol := internal.GetEnvString("OTEL_EXPORTER_OTLP_PROTOCOL",
		internal.GetEnvString("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf"))
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
	if internal.GetEnvBool("PEERDB_OTEL_METRICS_PANIC_ON_EXPORT_FAILURE", false) {
		return &panicOnFailureExporter{metricExporter}, err
	}
	return metricExporter, err
}

func setupMetricsProvider(
	ctx context.Context,
	otelResource *resource.Resource,
	enabled bool,
	views ...sdkmetric.View,
) (metric.MeterProvider, error) {
	if !enabled {
		return noop.NewMeterProvider(), nil
	}
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

func SetupPeerDBMetricsProvider(ctx context.Context, otelServiceName string, enabled bool) (metric.MeterProvider, error) {
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}
	return setupMetricsProvider(ctx, otelResource, enabled)
}

func SetupTemporalMetricsProvider(ctx context.Context, otelServiceName string, enabled bool) (metric.MeterProvider, error) {
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}
	return setupMetricsProvider(ctx, otelResource, enabled, temporalMetricsFilteringView())
}

func SetupComponentMetricsProvider(
	ctx context.Context,
	otelServiceName string,
	componentName string,
	enabled bool,
) (metric.MeterProvider, error) {
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}
	return setupMetricsProvider(ctx, otelResource, enabled, componentMetricsRenamingView(componentName))
}
