package connbigquery

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/stats"
)

func TestMeteredClientGRPCStatsHandler(t *testing.T) {
	var counter atomic.Int64
	ctx := withByteCounter(t.Context(), &counter)
	handler := &meteredGRPCStatsHandler{}

	handler.HandleRPC(ctx, &stats.InPayload{Client: true, WireLength: 42})
	handler.HandleRPC(ctx, &stats.InPayload{Client: true, WireLength: 58})
	assert.Equal(t, int64(100), counter.Load())

	// Outbound and server-side payloads aren't bytes fetched from BigQuery.
	handler.HandleRPC(ctx, &stats.OutPayload{Client: true, WireLength: 50})
	handler.HandleRPC(ctx, &stats.InPayload{Client: false, WireLength: 50})
	assert.Equal(t, int64(100), counter.Load())

	// RPCs outside a metered pull should not affect another pull's counter.
	handler.HandleRPC(context.Background(), &stats.InPayload{Client: true, WireLength: 50})
	assert.Equal(t, int64(100), counter.Load())
}
