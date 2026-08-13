package otel_metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

type capturingInt64Gauge struct {
	embedded.Int64Gauge
	values []int64
	attrs  []attribute.Set
}

func (g *capturingInt64Gauge) Enabled(context.Context) bool { return true }

func (g *capturingInt64Gauge) Record(ctx context.Context, value int64, options ...metric.RecordOption) {
	g.values = append(g.values, value)
	g.attrs = append(g.attrs, metric.NewRecordConfig(options).Attributes())
}

func flowContext(flowName string) context.Context {
	ctx := context.WithValue(context.Background(), internal.FlowMetadataKey, &protos.FlowContextMetadata{
		FlowName: flowName,
		Source: &protos.PeerContextMetadata{
			Name: "src", Type: protos.DBType_MONGO, Hostname: "src-host",
		},
		Destination: &protos.PeerContextMetadata{
			Name: "dst", Type: protos.DBType_CLICKHOUSE, Hostname: "dst-host",
		},
		Status: protos.FlowStatus_STATUS_RUNNING,
	})
	return internal.WithOperationContext(ctx, protos.FlowOperation_FLOW_OPERATION_SYNC)
}

func attrValue(t *testing.T, attrs attribute.Set, key string) string {
	t.Helper()
	value, ok := attrs.Value(attribute.Key(key))
	require.True(t, ok, "missing attribute %s", key)
	return value.String()
}

func TestContextAwareInt64SyncGaugeRecordsContextualAttributes(t *testing.T) {
	inner := &capturingInt64Gauge{}
	gauge := &ContextAwareInt64SyncGauge{Int64Gauge: inner}

	gauge.Record(flowContext("test-flow"), 42)

	require.Equal(t, []int64{42}, inner.values)
	require.Equal(t, "test-flow", attrValue(t, inner.attrs[0], FlowNameKey))
	require.Equal(t, "src", attrValue(t, inner.attrs[0], SourcePeerName))
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_SYNC.String(),
		attrValue(t, inner.attrs[0], FlowOperationKey))
}

func TestContextAttributesCache(t *testing.T) {
	inner := &capturingInt64Gauge{}
	gauge := &ContextAwareInt64SyncGauge{Int64Gauge: inner}

	// same context twice: identical attributes (served from cache)
	ctxA := flowContext("flow-a")
	gauge.Record(ctxA, 1)
	gauge.Record(ctxA, 2)
	require.Equal(t, inner.attrs[0], inner.attrs[1])
	require.Equal(t, "flow-a", attrValue(t, inner.attrs[1], FlowNameKey))

	// a different context must not be served stale attributes
	gauge.Record(flowContext("flow-b"), 3)
	require.Equal(t, "flow-b", attrValue(t, inner.attrs[2], FlowNameKey))

	// switching back rebuilds correctly too
	gauge.Record(ctxA, 4)
	require.Equal(t, "flow-a", attrValue(t, inner.attrs[3], FlowNameKey))

	// a derived context with a different operation is a different context value
	normalizeCtx := internal.WithOperationContext(ctxA, protos.FlowOperation_FLOW_OPERATION_NORMALIZE)
	gauge.Record(normalizeCtx, 5)
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_NORMALIZE.String(),
		attrValue(t, inner.attrs[4], FlowOperationKey))
}

func BenchmarkContextAwareGaugeRecord(b *testing.B) {
	meter := noop.NewMeterProvider().Meter("bench")
	inner, err := NewInt64SyncGauge(meter, "bench_gauge")
	require.NoError(b, err)
	gauge := &ContextAwareInt64SyncGauge{Int64Gauge: inner}
	ctx1 := flowContext("bench-flow")
	ctx2 := flowContext("bench-flow")

	b.Run("cache-hit", func(b *testing.B) {
		for b.Loop() {
			gauge.Record(ctx1, 1)
		}
	})
	b.Run("cache-miss", func(b *testing.B) {
		for b.Loop() {
			gauge.attrsCache.last.Store(&contextAttributesCacheEntry{ctx: ctx2})
			gauge.Record(ctx1, 1)
		}
	})
}
