package connsnowflake

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
)

// Metric name constants for the Snowpipe Streaming path.
// All names are prefixed by BuildMetricName (e.g. "peerdb.snowpipe_bytes_sent").
const (
	snowpipeBytesSentCounterName      = "snowpipe_bytes_sent"
	snowpipeRowsConfirmedCounterName  = "snowpipe_rows_confirmed"
	snowpipeRowsErroredCounterName    = "snowpipe_rows_errored"
	snowpipeChunkRetriesCounterName   = "snowpipe_chunk_retries"
	snowpipeIngestionLatencyGaugeName = "snowpipe_ingestion_latency_ms"
	// snowpipeCommitLagGaugeName is the client-perceived commit lag: wall-clock
	// time from flush start (sendChunkWithRetry) to ingestion confirmation
	// (waitForChannelIngestion). Includes network RTT, polling interval, and
	// Snowflake-side processing latency. Complements snowpipe_ingestion_latency_ms
	// which only reports the Snowflake server-side average.
	snowpipeCommitLagGaugeName = "snowpipe_commit_lag_ms"

	// snowpipeRetryReasonKey is the attribute key added to snowpipeChunkRetriesCounter
	// to distinguish retry causes: "transport", "channel_reopen", "auth", "transient".
	snowpipeRetryReasonKey = "retry_reason"
)

// streamingMetrics holds OpenTelemetry instruments for the Snowpipe Streaming path.
// All instruments are context-aware — flow metadata (flowName, peerType, etc.) is
// injected automatically from the context on every emission.
type streamingMetrics struct {
	// bytesSentCounter counts bytes successfully delivered to Snowpipe per chunk.
	bytesSentCounter metric.Int64Counter
	// rowsConfirmedCounter counts rows confirmed ingested by Snowflake (via waitForChannelIngestion).
	rowsConfirmedCounter metric.Int64Counter
	// rowsErroredCounter counts rows rejected by Snowflake (RowsErrorCount > 0 in channel status).
	rowsErroredCounter metric.Int64Counter
	// chunkRetriesCounter counts retry attempts in sendChunkWithRetry, tagged by retry_reason.
	chunkRetriesCounter metric.Int64Counter
	// ingestionLatencyGauge records Snowflake's reported avg processing latency (ms) per flush.
	ingestionLatencyGauge metric.Int64Gauge
	// commitLagGauge records the client-perceived end-to-end commit lag (ms) per flush:
	// elapsed wall-clock time from flush start to ingestion confirmation.
	commitLagGauge metric.Int64Gauge
}

// newStreamingMetrics initialises instruments from the global OTel meter provider.
// If the provider is not yet configured (startup race or tests without OTel setup),
// the SDK returns noop instruments — all Record/Add calls become no-ops.
func newStreamingMetrics() *streamingMetrics {
	meter := otel.GetMeterProvider().Meter("io.peerdb.flow-worker")

	bytesSent, err := otel_metrics.NewContextAwareInt64Counter(meter,
		otel_metrics.BuildMetricName(snowpipeBytesSentCounterName),
		metric.WithUnit("By"),
		metric.WithDescription("Bytes sent via Snowpipe Streaming REST API per chunk"),
	)
	if err != nil {
		bytesSent = noop.Int64Counter{}
	}

	rowsConfirmed, err := otel_metrics.NewContextAwareInt64Counter(meter,
		otel_metrics.BuildMetricName(snowpipeRowsConfirmedCounterName),
		metric.WithDescription("Rows confirmed ingested by Snowflake via Snowpipe Streaming"),
	)
	if err != nil {
		rowsConfirmed = noop.Int64Counter{}
	}

	rowsErrored, err := otel_metrics.NewContextAwareInt64Counter(meter,
		otel_metrics.BuildMetricName(snowpipeRowsErroredCounterName),
		metric.WithDescription("Rows rejected by Snowflake during Snowpipe Streaming ingestion"),
	)
	if err != nil {
		rowsErrored = noop.Int64Counter{}
	}

	chunkRetries, err := otel_metrics.NewContextAwareInt64Counter(meter,
		otel_metrics.BuildMetricName(snowpipeChunkRetriesCounterName),
		metric.WithDescription("Retry attempts in Snowpipe Streaming chunk send, tagged by retry_reason"),
	)
	if err != nil {
		chunkRetries = noop.Int64Counter{}
	}

	ingestionLatency, err := otel_metrics.ContextAwareInt64Gauge(meter,
		otel_metrics.BuildMetricName(snowpipeIngestionLatencyGaugeName),
		metric.WithUnit("ms"),
		metric.WithDescription("Snowflake-reported average processing latency for Snowpipe Streaming"),
	)
	if err != nil {
		ingestionLatency = noop.Int64Gauge{}
	}

	commitLag, err := otel_metrics.ContextAwareInt64Gauge(meter,
		otel_metrics.BuildMetricName(snowpipeCommitLagGaugeName),
		metric.WithUnit("ms"),
		metric.WithDescription("Client-perceived commit lag for Snowpipe Streaming flush (start → confirmation)"),
	)
	if err != nil {
		commitLag = noop.Int64Gauge{}
	}

	return &streamingMetrics{
		bytesSentCounter:      bytesSent,
		rowsConfirmedCounter:  rowsConfirmed,
		rowsErroredCounter:    rowsErrored,
		chunkRetriesCounter:   chunkRetries,
		ingestionLatencyGauge: ingestionLatency,
		commitLagGauge:        commitLag,
	}
}
