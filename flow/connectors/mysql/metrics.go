package connmysql

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
)

const usedMySQLCharsetsName = "used_mysql_charsets"

type metrics struct {
	usedCharsetsCounter metric.Int64Counter
}

// newMetrics initializes the MySQL-specific instruments through the shared
// OtelManager.
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
