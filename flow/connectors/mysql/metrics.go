package connmysql

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
)

// usedMySQLCharsetsName is the base name of the MySQL-only charset usage counter.
const usedMySQLCharsetsName = "used_mysql_charsets"

// metrics holds MySQL-specific OpenTelemetry instruments. Keeping their
// definitions in the connector package (rather than the shared otel_metrics
// package) keeps MySQL-only metric changes scoped to flow/connectors/mysql.
type metrics struct {
	usedCharsetsCounter metric.Int64Counter
}

// newMetrics initializes the MySQL-specific instruments through the shared
// OtelManager. It must be called once from a single-threaded path (e.g. at the
// start of PullRecords) because the manager's instrument caches are not
// concurrency-safe.
func newMetrics(om *otel_metrics.OtelManager) (*metrics, error) {
	usedCharsetsCounter, err := om.GetOrInitInt64Counter(
		otel_metrics.BuildMetricName(usedMySQLCharsetsName),
		metric.WithDescription(
			"Counter of used MySQL charsets, with `charset` label and `status` label indicating unsupported/transcoded/not_transcoded"),
	)
	if err != nil {
		return nil, err
	}
	return &metrics{usedCharsetsCounter: usedCharsetsCounter}, nil
}

func (m *metrics) recordUsedCharset(ctx context.Context, charset string, status string) {
	m.usedCharsetsCounter.Add(ctx, 1,
		metric.WithAttributeSet(attribute.NewSet(
			attribute.String("charset", charset),
			attribute.String("status", status),
		)),
	)
}
