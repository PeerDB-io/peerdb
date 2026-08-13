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

	ctxA := flowContext("flow-a")
	ctxB := flowContext("flow-b")
	derivedCtxA := internal.WithOperationContext(ctxA, protos.FlowOperation_FLOW_OPERATION_NORMALIZE)

	// same context twice: identical attributes (served from cache)
	gauge.Record(ctxA, 1)
	gauge.Record(ctxA, 2)
	require.Equal(t, inner.attrs[0], inner.attrs[1])
	require.Equal(t, "flow-a", attrValue(t, inner.attrs[1], FlowNameKey))
	entryA, ok := gauge.attrsCache.entries.Load("flow-a")
	require.True(t, ok)

	// a different flow's context must not be served stale attributes
	gauge.Record(ctxB, 3)
	require.Equal(t, "flow-b", attrValue(t, inner.attrs[2], FlowNameKey))

	// switching back rebuilds correctly too (and served from cache)
	gauge.Record(ctxA, 4)
	require.Equal(t, "flow-a", attrValue(t, inner.attrs[3], FlowNameKey))
	entryA2, ok := gauge.attrsCache.entries.Load("flow-a")
	require.True(t, ok)
	require.Same(t, entryA, entryA2)

	// a derived context with a different operation is a different context value
	gauge.Record(derivedCtxA, 5)
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_NORMALIZE.String(),
		attrValue(t, inner.attrs[4], FlowOperationKey))

	// the original context is unaffected
	gauge.Record(ctxA, 6)
	require.Equal(t, "flow-a", attrValue(t, inner.attrs[5], FlowNameKey))
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_SYNC.String(),
		attrValue(t, inner.attrs[5], FlowOperationKey))
}

func TestContextAttributesWithoutFlowName(t *testing.T) {
	inner := &capturingInt64Gauge{}
	gauge := &ContextAwareInt64SyncGauge{Int64Gauge: inner}

	_, ok := gauge.attrsCache.entries.Load("")
	require.False(t, ok) // cache is empty

	ctx := context.Background()
	gauge.Record(ctx, 1)
	_, hasFlowName := inner.attrs[0].Value(FlowNameKey)
	require.False(t, hasFlowName)
	_, ok = gauge.attrsCache.entries.Load("")
	require.True(t, ok) // cache is populated

	// same context is served from cache
	gauge.Record(ctx, 2)
	require.Equal(t, inner.attrs[0], inner.attrs[1])

	// different context (also without flow name) rebuilds its own attributes
	otherCtx := internal.WithOperationContext(context.Background(), protos.FlowOperation_FLOW_OPERATION_SYNC)
	gauge.Record(otherCtx, 3)
	require.Equal(t, protos.FlowOperation_FLOW_OPERATION_SYNC.String(),
		attrValue(t, inner.attrs[2], FlowOperationKey))
	_, hasFlowName = inner.attrs[2].Value(FlowNameKey)
	require.False(t, hasFlowName)
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
			gauge.attrsCache.entries.Store("bench-flow", &contextAttributesCacheEntry{ctx: ctx2})
			gauge.Record(ctx1, 1)
		}
	})

	interleavedCtxs := []context.Context{
		flowContext("bench-interleaved-1"),
		flowContext("bench-interleaved-2"),
		flowContext("bench-interleaved-3"),
	}
	b.Run("cache-hit-interleaved-ctx", func(b *testing.B) {
		i := 0
		for b.Loop() {
			gauge.Record(interleavedCtxs[i%len(interleavedCtxs)], 1)
			i++
		}
	})
}
