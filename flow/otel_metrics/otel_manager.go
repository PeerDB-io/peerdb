package otel_metrics

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

const (
	FlowWorkerServiceName         = "flow-worker"
	FlowSnapshotWorkerServiceName = "flow-snapshot-worker"
	FlowApiServiceName            = "flow-api"
)

const (
	SlotLagGaugeName                     = "cdc_slot_lag"
	CurrentBatchIdGaugeName              = "current_batch_id"
	LastNormalizedBatchIdGaugeName       = "last_normalized_batch_id"
	OpenConnectionsGaugeName             = "open_connections"
	OpenReplicationConnectionsGaugeName  = "open_replication_connections"
	CommittedLSNGaugeName                = "committed_lsn"
	RestartLSNGaugeName                  = "restart_lsn"
	ConfirmedFlushLSNGaugeName           = "confirmed_flush_lsn"
	SentLSNGaugeName                     = "sent_lsn"
	ReceivedCommitLSNGaugeName           = "received_commit_lsn"
	CurrentWalLSNGaugeName               = "current_wal_lsn"
	RestartToConfirmedMBGaugeName        = "restart_to_confirmed_lsn"
	ConfirmedToCurrentMBGaugeName        = "confirmed_to_current_lsn"
	WalStatusGaugeName                   = "wal_status"
	SafeWalSizeGaugeName                 = "safe_wal_size"
	SlotActiveGaugeName                  = "slot_active"
	WalSenderStateGaugeName              = "walsender_state"
	LogicalDecodingWorkMemGaugeName      = "logical_decoding_work_mem"
	StatsResetGaugeName                  = "stats_reset"
	SpillTxnsGaugeName                   = "spill_txns"
	SpillCountGaugeName                  = "spill_count"
	SpillBytesGaugeName                  = "spill_bytes"
	IntervalSinceLastNormalizeGaugeName  = "interval_since_last_normalize"
	AllFetchedBytesCounterName           = "all_fetched_bytes"
	FetchedBytesCounterName              = "fetched_bytes"
	SourceLagGaugeName                   = "source_lag"
	DestinationLagGaugeName              = "destination_lag"
	E2ELagGaugeName                      = "e2e_lag"
	ServerSideCommitLagGaugeName         = "server_side_commit_lag"
	NormalizeLagGaugeName                = "normalize_lag"
	ErrorEmittedGaugeName                = "error_emitted"
	ErrorsEmittedCounterName             = "errors_emitted"
	WarningEmittedGaugeName              = "warning_emitted"
	WarningsEmittedCounterName           = "warnings_emitted"
	RecordsSyncedGaugeName               = "records_synced"
	RecordsSyncedCounterName             = "records_synced_counter"
	RecordsSyncedPerTableGaugeName       = "records_synced_per_table"
	RecordsSyncedPerTableCounterName     = "records_synced_per_table_counter"
	SyncedTablesGaugeName                = "synced_tables"
	InstanceStatusGaugeName              = "instance_status"
	MaintenanceStatusGaugeName           = "maintenance_status"
	FlowStatusGaugeName                  = "flow_status"
	DurationSinceLastFlowUpdateGaugeName = "duration_since_last_flow_update"
	ActiveFlowsGaugeName                 = "active_flows"
	CPULimitsPerActiveFlowGaugeName      = "cpu_limits_per_active_flow_vcores"
	MemoryLimitsPerActiveFlowGaugeName   = "memory_limits_per_active_flow"
	TotalCPULimitsGaugeName              = "total_cpu_limits_vcores"
	TotalMemoryLimitsGaugeName           = "total_memory_limits"
	WorkloadTotalReplicasGaugeName       = "workload_total_replicas"
	LogRetentionGaugeName                = "log_retention"
	LatestConsumedLogEventGaugeName      = "latest_consumed_log_event"
	UnchangedToastValuesCounterName      = "unchanged_toast_values"
	CodeNotificationCounterName          = "code_notification"
	ServerWalEndLagGaugeName             = "wal_end_lag"
	UsedMySQLCharsetsName                = "used_mysql_charsets"
	ColumnTypeChangesName                = "column_type_changes"
	ParseSQLErrorsCounterName            = "parse_sql_errors"
	OnlineSchemaMigrationsName           = "online_schema_migrations"
	UnsupportedBinlogEventName           = "unsupported_binlog_event"
)

type Metrics struct {
	SlotLagGauge                     metric.Float64Gauge
	CurrentBatchIdGauge              metric.Int64Gauge
	LastNormalizedBatchIdGauge       metric.Int64Gauge
	OpenConnectionsGauge             metric.Int64Gauge
	OpenReplicationConnectionsGauge  metric.Int64Gauge
	CommittedLSNGauge                metric.Int64Gauge
	RestartLSNGauge                  metric.Int64Gauge
	ConfirmedFlushLSNGauge           metric.Int64Gauge
	SentLSNGauge                     metric.Int64Gauge
	ReceivedCommitLSNGauge           metric.Int64Gauge
	CurrentWalLSNGauge               metric.Int64Gauge
	RestartToConfirmedMBGauge        metric.Float64Gauge
	ConfirmedToCurrentMBGauge        metric.Float64Gauge
	WalStatusGauge                   metric.Int64Gauge
	SafeWalSizeGauge                 metric.Int64Gauge
	SlotActiveGauge                  metric.Int64Gauge
	WalSenderStateGauge              metric.Int64Gauge
	StatsResetGauge                  metric.Int64Gauge
	SpillTxnsGauge                   metric.Int64Gauge
	SpillCountGauge                  metric.Int64Gauge
	SpillBytesGauge                  metric.Int64Gauge
	LogicalDecodingWorkMemGauge      metric.Int64Gauge
	IntervalSinceLastNormalizeGauge  metric.Float64Gauge
	AllFetchedBytesCounter           metric.Int64Counter
	FetchedBytesCounter              metric.Int64Counter
	SourceLagGauge                   metric.Int64Gauge
	DestinationLagGauge              metric.Int64Gauge
	E2ELagGauge                      metric.Int64Gauge
	ServerSideCommitLagGauge         metric.Int64Gauge
	NormalizeLagGauge                metric.Int64Gauge
	ErrorEmittedGauge                metric.Int64Gauge
	ErrorsEmittedCounter             metric.Int64Counter
	WarningsEmittedGauge             metric.Int64Gauge
	WarningEmittedCounter            metric.Int64Counter
	RecordsSyncedGauge               metric.Int64Gauge
	RecordsSyncedCounter             metric.Int64Counter
	RecordsSyncedPerTableGauge       metric.Int64Gauge
	RecordsSyncedPerTableCounter     metric.Int64Counter
	SyncedTablesGauge                metric.Int64Gauge
	InstanceStatusGauge              metric.Int64Gauge
	MaintenanceStatusGauge           metric.Int64Gauge
	FlowStatusGauge                  metric.Int64Gauge
	DurationSinceLastFlowUpdateGauge metric.Int64Gauge
	ActiveFlowsGauge                 metric.Int64Gauge
	CPULimitsPerActiveFlowGauge      metric.Float64Gauge
	MemoryLimitsPerActiveFlowGauge   metric.Float64Gauge
	TotalCPULimitsGauge              metric.Float64Gauge
	TotalMemoryLimitsGauge           metric.Float64Gauge
	WorkloadTotalReplicasGauge       metric.Int64Gauge
	LatestConsumedLogEventGauge      metric.Int64Gauge
	LogRetentionGauge                metric.Float64Gauge
	UnchangedToastValuesCounter      metric.Int64Counter
	ServerWalEndLagGauge             metric.Int64Gauge
	UsedMySQLCharsetsCounter         metric.Int64Counter
	ColumnTypeChangesCounter         metric.Int64Counter
	ParseSQLErrorsCounter            metric.Int64Counter
	OnlineSchemaMigrationsCounter    metric.Int64Counter
	UnsupportedBinlogEventCounter    metric.Int64Counter
}

type SlotMetricGauges struct {
	SlotLagGauge                    metric.Float64Gauge
	RestartLSNGauge                 metric.Int64Gauge
	ConfirmedFlushLSNGauge          metric.Int64Gauge
	SentLSNGauge                    metric.Int64Gauge
	CurrentWalLSNGauge              metric.Int64Gauge
	RestartToConfirmedMBGauge       metric.Float64Gauge
	ConfirmedToCurrentMBGauge       metric.Float64Gauge
	WalStatusGauge                  metric.Int64Gauge
	SafeWalSizeGauge                metric.Int64Gauge
	SlotActiveGauge                 metric.Int64Gauge
	WalSenderStateGauge             metric.Int64Gauge
	StatsResetGauge                 metric.Int64Gauge
	SpillTxnsGauge                  metric.Int64Gauge
	SpillCountGauge                 metric.Int64Gauge
	SpillBytesGauge                 metric.Int64Gauge
	LogicalDecodingWorkMemGauge     metric.Int64Gauge
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
	Tracer             trace.Tracer
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
		Tracer:             Tracer(),
		Float64GaugesCache: make(map[string]metric.Float64Gauge),
		Int64GaugesCache:   make(map[string]metric.Int64Gauge),
		Int64CountersCache: make(map[string]metric.Int64Counter),
	}
	if err := otelManager.setupMetrics(ctx); err != nil {
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

// CodeNotificationCounter is a global counter for emitting notifications for one-off things we want to know about with the least effort.
// In ClickPipes, there is a generic (non-paging) alert set up on this, so just emit it in the code with a unique message
// and it'll show up on Slack.
// It is intentionally global for ease of use, all others are supposed to be passed through as usual.
var CodeNotificationCounter metric.Int64Counter = noop.Int64Counter{}

func (om *OtelManager) setupMetrics(ctx context.Context) error {
	slog.DebugContext(ctx, "Setting up all metrics")
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

	if om.Metrics.SentLSNGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(SentLSNGaugeName),
		metric.WithDescription("Sent LSN from pg_stat_replication, only emitted if we have pg_monitor/pg_read_all_stats role"),
	); err != nil {
		return err
	}

	if om.Metrics.ReceivedCommitLSNGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(ReceivedCommitLSNGaugeName),
		metric.WithDescription("Received commit LSN on the consumer side"),
	); err != nil {
		return err
	}

	if om.Metrics.CurrentWalLSNGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(CurrentWalLSNGaugeName),
		metric.WithDescription("Current WAL LSN from pg_current_wal_lsn or pg_last_wal_receive_lsn"),
	); err != nil {
		return err
	}

	if om.Metrics.RestartToConfirmedMBGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(RestartToConfirmedMBGaugeName),
		metric.WithUnit("MiBy"),
		metric.WithDescription("Difference between confirmed_flush_lsn and restart_lsn (MB)"),
	); err != nil {
		return err
	}

	if om.Metrics.ConfirmedToCurrentMBGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(ConfirmedToCurrentMBGaugeName),
		metric.WithUnit("MiBy"),
		metric.WithDescription("Difference between sent_lsn and current WAL LSN (MB)"),
	); err != nil {
		return err
	}

	if om.Metrics.WalStatusGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(WalStatusGaugeName),
		metric.WithDescription("WAL status of the replication slot (value 1 with status as attribute)"),
	); err != nil {
		return err
	}

	if om.Metrics.SafeWalSizeGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(SafeWalSizeGaugeName),
		metric.WithUnit("By"),
		metric.WithDescription("Slot's safe_wal_size field (available PG13+)"),
	); err != nil {
		return err
	}

	if om.Metrics.SlotActiveGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(SlotActiveGaugeName),
		metric.WithDescription("Whether the replication slot is currently active (0 or 1)"),
	); err != nil {
		return err
	}

	if om.Metrics.WalSenderStateGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(WalSenderStateGaugeName),
		metric.WithDescription("Indicates walsender's current wait or I/O state. Value always 1."),
	); err != nil {
		return err
	}

	if om.Metrics.StatsResetGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(StatsResetGaugeName),
		metric.WithUnit("s"),
		metric.WithDescription("Unix timestamp when pg_stat_replication_slots statistics were last reset (PG16+)"),
	); err != nil {
		return err
	}

	if om.Metrics.SpillTxnsGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(SpillTxnsGaugeName),
		metric.WithDescription("Current number of transactions spilled to disk (PG16+)"),
	); err != nil {
		return err
	}

	if om.Metrics.SpillCountGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(SpillCountGaugeName),
		metric.WithDescription("Current number of spill events (PG16+)"),
	); err != nil {
		return err
	}

	if om.Metrics.SpillBytesGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(SpillBytesGaugeName),
		metric.WithUnit("By"),
		metric.WithDescription("Current bytes spilled due to logical_decoding_work_mem exhaustion (PG16+)"),
	); err != nil {
		return err
	}

	if om.Metrics.LogicalDecodingWorkMemGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(LogicalDecodingWorkMemGaugeName),
		metric.WithUnit("MiBy"),
		metric.WithDescription("Current logical_decoding_work_mem setting in MB"),
	); err != nil {
		return err
	}

	if om.Metrics.IntervalSinceLastNormalizeGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(IntervalSinceLastNormalizeGaugeName),
		metric.WithUnit("s"),
		metric.WithDescription("Interval since last normalize"),
	); err != nil {
		return err
	}

	if om.Metrics.AllFetchedBytesCounter, err = om.GetOrInitInt64Counter(BuildMetricName(AllFetchedBytesCounterName),
		metric.WithUnit("By"),
		metric.WithDescription("Bytes received of CopyData over replication protocol for all tables"),
	); err != nil {
		return err
	}

	if om.Metrics.FetchedBytesCounter, err = om.GetOrInitInt64Counter(BuildMetricName(FetchedBytesCounterName),
		metric.WithUnit("By"),
		metric.WithDescription("Bytes received of CopyData over replication protocol for mapped tables only"),
	); err != nil {
		return err
	}

	if om.Metrics.SourceLagGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(SourceLagGaugeName),
		metric.WithUnit("ms"),
		metric.WithDescription("Lag in milliseconds from a source event's commit timestamp to when PeerDB receives it"),
	); err != nil {
		return err
	}

	if om.Metrics.DestinationLagGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(DestinationLagGaugeName),
		metric.WithUnit("ms"),
		metric.WithDescription("Lag in milliseconds from when PeerDB receives a source event to when it is written to the destination"),
	); err != nil {
		return err
	}

	if om.Metrics.E2ELagGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(E2ELagGaugeName),
		metric.WithUnit("ms"),
		metric.WithDescription("End-to-end lag in milliseconds from a source event's commit timestamp to destination write"),
	); err != nil {
		return err
	}

	if om.Metrics.ServerSideCommitLagGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(ServerSideCommitLagGaugeName),
		metric.WithUnit("us"),
		metric.WithDescription("Similar to CommitLagGauge, but use source-only timestamps to avoid clock skew"),
	); err != nil {
		return err
	}

	if om.Metrics.NormalizeLagGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(NormalizeLagGaugeName),
		metric.WithUnit("us"),
		metric.WithDescription("Lag in microseconds for batches that are synced but not normalized"),
	); err != nil {
		return err
	}

	if om.Metrics.LatestConsumedLogEventGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(LatestConsumedLogEventGaugeName),
		metric.WithUnit("s"),
		metric.WithDescription("Latest consumed replication log event timestamp in epoch seconds"),
	); err != nil {
		return err
	}

	if om.Metrics.LogRetentionGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(LogRetentionGaugeName),
		metric.WithUnit("h"),
		metric.WithDescription("Log retention in hours for the source data store"),
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
		// This the actual counter for errors emitted, used for alerting based on error rate, or using more detailed error analysis
		metric.WithDescription("Counter of errors emitted"),
	); err != nil {
		return err
	}

	if om.Metrics.WarningsEmittedGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(WarningEmittedGaugeName),
		// This mostly tells whether warning is emitted or not, used for hooking up event based alerting
		metric.WithDescription("Whether warning was emitted, 1 if emitted, 0 otherwise"),
	); err != nil {
		return err
	}

	if om.Metrics.WarningEmittedCounter, err = om.GetOrInitInt64Counter(BuildMetricName(WarningsEmittedCounterName),
		// This the actual counter for warnings emitted, used for alerting based on warning rate, or using more detailed error analysis
		metric.WithDescription("Counter of warnings emitted"),
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

	if om.Metrics.RecordsSyncedPerTableGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(RecordsSyncedPerTableGaugeName),
		metric.WithDescription("Number of records synced per table. Note that this should be monotonically increasing"),
	); err != nil {
		return err
	}

	if om.Metrics.RecordsSyncedPerTableCounter, err = om.GetOrInitInt64Counter(BuildMetricName(RecordsSyncedPerTableCounterName),
		metric.WithDescription("Counter of records synced per table (all time)"),
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
	slog.DebugContext(ctx, "Finished setting up all metrics")

	if om.Metrics.FlowStatusGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(FlowStatusGaugeName),
		metric.WithDescription("Status of the flow, always emits a 1 metric with different `flowStatus` value for different statuses"),
	); err != nil {
		return err
	}

	if om.Metrics.DurationSinceLastFlowUpdateGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(DurationSinceLastFlowUpdateGaugeName),
		metric.WithUnit("s"),
		metric.WithDescription("Duration since last flow update in seconds"),
	); err != nil {
		return err
	}

	if om.Metrics.ActiveFlowsGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(ActiveFlowsGaugeName),
		metric.WithDescription("Number of active flows"),
	); err != nil {
		return err
	}

	// Appending unit since UCUM does not support `vcores` as a unit
	if om.Metrics.CPULimitsPerActiveFlowGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(CPULimitsPerActiveFlowGaugeName),
		metric.WithDescription(
			"CPU limits per active flow. To get total CPU limits, multiply by number of active flows or do sum over all flows",
		),
	); err != nil {
		return err
	}

	if om.Metrics.MemoryLimitsPerActiveFlowGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(MemoryLimitsPerActiveFlowGaugeName),
		metric.WithDescription(
			"Memory per active flow. To get total memory limits, multiply by number of active flows or do sum over all flows",
		),
		metric.WithUnit("By"),
	); err != nil {
		return err
	}

	if om.Metrics.TotalCPULimitsGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(TotalCPULimitsGaugeName),
		metric.WithDescription("Total CPU limits for the current workload"),
	); err != nil {
		return err
	}

	if om.Metrics.TotalMemoryLimitsGauge, err = om.GetOrInitFloat64Gauge(BuildMetricName(TotalMemoryLimitsGaugeName),
		metric.WithDescription("Total memory limits for the current workload"),
		metric.WithUnit("By"),
	); err != nil {
		return err
	}

	if om.Metrics.WorkloadTotalReplicasGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(WorkloadTotalReplicasGaugeName),
		metric.WithDescription("Total number of replicas for the current workload"),
	); err != nil {
		return err
	}

	if om.Metrics.UnchangedToastValuesCounter, err = om.GetOrInitInt64Counter(BuildMetricName(UnchangedToastValuesCounterName),
		metric.WithDescription(
			"Counter of unchanged TOAST values (Postgres only), with `backfilled` indicating whether the original was found in the CDC store"),
	); err != nil {
		return err
	}

	if om.Metrics.UsedMySQLCharsetsCounter, err = om.GetOrInitInt64Counter(BuildMetricName(UsedMySQLCharsetsName),
		metric.WithDescription(
			"Counter of used MySQL charsets, with `charset` label and `status` label indicating unsupported/transcoded/not_transcoded"),
	); err != nil {
		return err
	}

	if om.Metrics.ColumnTypeChangesCounter, err = om.GetOrInitInt64Counter(BuildMetricName(ColumnTypeChangesName),
		metric.WithDescription(
			"Counter of column type changes detected on the CDC path, with `source` label holding the source peer type, "+
				"`from`/`to` labels holding the source/target type and `sourceEventType` holding the source of event(ddl, eventMetadata)"),
	); err != nil {
		return err
	}

	if om.Metrics.ParseSQLErrorsCounter, err = om.GetOrInitInt64Counter(BuildMetricName(ParseSQLErrorsCounterName),
		metric.WithDescription("Counter of errors encountered while parsing MySQL QueryEvent SQL on the CDC path")); err != nil {
		return err
	}

	if om.Metrics.OnlineSchemaMigrationsCounter, err = om.GetOrInitInt64Counter(BuildMetricName(OnlineSchemaMigrationsName),
		metric.WithDescription(
			"Counter of online schema migrations detected on the CDC path, i.e. a tracked table being atomically "+
				"renamed into by a shadow/ghost table, with `source` label holding the source peer type and `tool` "+
				"label holding the detected migration tool (gh-ost, pt-online-schema-change, other)"),
	); err != nil {
		return err
	}

	if om.Metrics.UnsupportedBinlogEventCounter, err = om.GetOrInitInt64Counter(BuildMetricName(UnsupportedBinlogEventName),
		metric.WithDescription(
			"Counter of unsupported binlog events seen on the CDC path, with `eventType` label "+
				"holding the numeric binlog event type"),
	); err != nil {
		return err
	}

	if CodeNotificationCounter, err = om.GetOrInitInt64Counter(BuildMetricName(CodeNotificationCounterName),
		metric.WithDescription("One-off notifications with unique `message` attribute, triggers generic non-paging alert"),
	); err != nil {
		return err
	}

	if om.Metrics.ServerWalEndLagGauge, err = om.GetOrInitInt64Gauge(BuildMetricName(ServerWalEndLagGaugeName),
		metric.WithUnit("By"),
		metric.WithDescription("Difference in bytes between the server's WAL end and the WAL end of the last "+
			"received XLogData; used in conjunction with slot lag to determine large transactions"),
	); err != nil {
		return err
	}

	return nil
}

// newOtelResource returns a resource describing this application.
func newOtelResource(otelServiceName string) (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes("",
			attribute.Key("service.name").String(otelServiceName),
			attribute.String(DeploymentUidKey, internal.PeerDBDeploymentUID()),
			attribute.Key("service.version").String(internal.PeerDBVersionShaShort()),
		),
	)
}

func temporalMetricsFilteringView(ctx context.Context) sdkmetric.View {
	exportListString := internal.GetPeerDBOtelTemporalMetricsExportListEnv()
	slog.InfoContext(ctx, "Found export list for temporal metrics", slog.String("exportList", exportListString))
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
	// otel v1.44.0 introduced a default max request size of 64 MiB, making oversized
	// exports fail as non-retryable errors; 0 disables it, preserving the previous
	// unlimited behavior
	switch otlpMetricProtocol {
	case "http/protobuf":
		metricExporter, err = otlpmetrichttp.New(ctx, otlpmetrichttp.WithMaxRequestSize(0))
	case "grpc":
		metricExporter, err = otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithMaxRequestSize(0))
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

func setupMetricsAndProvider(
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
	setupOtelHandlers(ctx)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(otelResource),
		sdkmetric.WithView(views...),
		// otel v1.44.0 introduced a default cardinality limit of 2000 per instrument;
		// 0 disables it, preserving the previous unlimited behavior
		sdkmetric.WithCardinalityLimit(0),
	)
	return meterProvider, nil
}

func SetupPeerDBMetricsProvider(ctx context.Context, otelServiceName string, enabled bool) (metric.MeterProvider, error) {
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}
	return setupMetricsAndProvider(ctx, otelResource, enabled)
}

func SetupTemporalMetricsProvider(ctx context.Context, otelServiceName string, enabled bool) (metric.MeterProvider, error) {
	otelResource, err := newOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}
	return setupMetricsAndProvider(ctx, otelResource, enabled, temporalMetricsFilteringView(ctx))
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
	return setupMetricsAndProvider(ctx, otelResource, enabled, componentMetricsRenamingView(componentName))
}

type LoggingErrorHandler struct {
	logger *slog.Logger
}

func NewLoggingErrorHandler(logger *slog.Logger) *LoggingErrorHandler {
	return &LoggingErrorHandler{
		logger: logger.With("component", "global-otel-error-handler"),
	}
}

func (l *LoggingErrorHandler) Handle(err error) {
	l.logger.Error("otel error", slog.Any("error", err)) //nolint:sloglint
}

func setupOtelHandlers(ctx context.Context) {
	logger := internal.SlogLoggerFromCtx(ctx)
	otel.SetErrorHandler(NewLoggingErrorHandler(logger))
	otel.SetLogger(logr.FromSlogHandler(logger.With("component", "global-otel-logger-handler").Handler()))
}
