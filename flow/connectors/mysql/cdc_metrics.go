package connmysql

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
)

// Shared with the common.Interval goroutine.
type mysqlPullCounters struct {
	otelManager       *otel_metrics.OtelManager
	charsetCounts     map[mysqlCharsetMetricKey]int64
	mu                sync.Mutex
	fetchedBytes      atomic.Int64
	totalFetchedBytes atomic.Int64
	allFetchedBytes   atomic.Int64
	receiveTime       atomic.Int64
	processTime       atomic.Int64
	addRecordTime     atomic.Int64
}

type mysqlCharsetMetricKey struct {
	charset string
	status  string
}

func newMysqlPullCounters(otelManager *otel_metrics.OtelManager) *mysqlPullCounters {
	return &mysqlPullCounters{otelManager: otelManager}
}

func (c *mysqlPullCounters) recordUsedCharset(charset, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.charsetCounts == nil {
		c.charsetCounts = make(map[mysqlCharsetMetricKey]int64)
	}
	c.charsetCounts[mysqlCharsetMetricKey{charset: charset, status: status}]++
}

func (c *mysqlPullCounters) flush(ctx context.Context) {
	// Serialize the interval callback and final flush, and protect charset counts.
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, count := range c.charsetCounts {
		c.otelManager.Metrics.UsedMySQLCharsetsCounter.Add(ctx, count, metric.WithAttributeSet(attribute.NewSet(
			attribute.String("charset", key.charset), attribute.String("status", key.status),
		)))
	}
	clear(c.charsetCounts)
	c.otelManager.Metrics.FetchedBytesCounter.Add(ctx, c.fetchedBytes.Swap(0))
	c.otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, c.allFetchedBytes.Swap(0))
	c.otelManager.Metrics.CDCReceiveTimeCounter.Add(ctx, c.receiveTime.Swap(0))
	c.otelManager.Metrics.CDCProcessTimeCounter.Add(ctx, c.processTime.Swap(0))
	c.otelManager.Metrics.CDCAddRecordTimeCounter.Add(ctx, c.addRecordTime.Swap(0))
}

// Accessed only from PullRecords, not shared with the common.Interval goroutine, so it needs no mutex.
type mysqlPullGauges struct {
	otelManager    *otel_metrics.OtelManager
	lastRecordedAt time.Time
	clockOffset    time.Duration
	// Latest observed values not yet recorded. Zero lag is valid, hence the flag.
	sourceLagMillis         int64
	sourceLagPending        bool
	latestConsumedTimestamp int64
}

func newMysqlPullGauges(otelManager *otel_metrics.OtelManager, clockOffset time.Duration) *mysqlPullGauges {
	return &mysqlPullGauges{otelManager: otelManager, clockOffset: clockOffset}
}

func (g *mysqlPullGauges) observeEvent(event *replication.BinlogEvent, receivedAt time.Time) {
	if _, isHeartbeat := event.Event.(*replication.HeartbeatEvent); isHeartbeat {
		// Clear backlog lag when the source becomes idle.
		g.sourceLagMillis = 0
		g.sourceLagPending = true
		return
	}

	// Prefer precise GTID commit timestamps over the seconds-resolution header.
	timestampMicros := int64(event.Header.Timestamp) * int64(time.Second/time.Microsecond)
	switch ev := event.Event.(type) {
	case *replication.GTIDEvent:
		if ev.ImmediateCommitTimestamp > 0 {
			timestampMicros = int64(ev.ImmediateCommitTimestamp)
		}
	case *replication.GtidTaggedLogEvent:
		if ev.ImmediateCommitTimestamp > 0 {
			timestampMicros = int64(ev.ImmediateCommitTimestamp)
		}
	}

	if timestampMicros > 0 {
		g.sourceLagMillis = receivedAt.Add(g.clockOffset).Sub(time.UnixMicro(timestampMicros)).Milliseconds()
		g.sourceLagPending = true
	}
}

func (g *mysqlPullGauges) observeConsumed(timestamp int64) {
	if timestamp > 0 {
		g.latestConsumedTimestamp = timestamp
	}
}

func (g *mysqlPullGauges) recordPeriodic(ctx context.Context, now time.Time) {
	// recording gauges per event is wasteful on CPU given this is on the hot path
	if now.Sub(g.lastRecordedAt) >= time.Second {
		g.recordPending(ctx)
		g.lastRecordedAt = now
	}
}

func (g *mysqlPullGauges) recordBatchEnd(ctx context.Context) {
	g.recordPending(ctx)
}

func (g *mysqlPullGauges) recordPending(ctx context.Context) {
	if g.sourceLagPending {
		g.otelManager.Metrics.SourceLagGauge.Record(ctx, g.sourceLagMillis)
		g.sourceLagPending = false
	}
	if g.latestConsumedTimestamp > 0 {
		g.otelManager.Metrics.LatestConsumedLogEventGauge.Record(ctx, g.latestConsumedTimestamp)
		g.latestConsumedTimestamp = 0
	}
}
