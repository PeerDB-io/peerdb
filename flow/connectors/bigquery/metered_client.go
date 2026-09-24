package connbigquery

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"

	"cloud.google.com/go/auth"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"google.golang.org/grpc/stats"
)

type byteCounterCtxKey struct{}

// withByteCounter returns a context that the metered HTTP and gRPC transports
// accumulate response payload bytes into. BigQuery's RowIterator keeps this
// context for paginated HTTP and Storage Read requests.
func withByteCounter(ctx context.Context, counter *atomic.Int64) context.Context {
	return context.WithValue(ctx, byteCounterCtxKey{}, counter)
}

// meteredGRPCStatsHandler counts incoming Storage Read payload bytes for RPCs
// whose context carries a byte counter. WireLength includes compression and the
// gRPC message frame, but not HTTP/2 framing.
type meteredGRPCStatsHandler struct{}

func (*meteredGRPCStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (*meteredGRPCStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	payload, ok := rpcStats.(*stats.InPayload)
	if !ok || !payload.Client {
		return
	}
	if counter, ok := ctx.Value(byteCounterCtxKey{}).(*atomic.Int64); ok {
		counter.Add(int64(payload.WireLength))
	}
}

func (*meteredGRPCStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*meteredGRPCStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

// meteredRoundTripper counts response body bytes consumed by the BigQuery client
// for requests whose context carries a byte counter (see withByteCounter), so
// BigQuery CDC pull can report bytes read instead of an approximation of the
// converted row size. net/http may transparently decompress the body before it
// reaches this wrapper.
type meteredRoundTripper struct {
	base http.RoundTripper
}

func (t *meteredRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if resp == nil {
		return nil, err
	}
	if counter, ok := req.Context().Value(byteCounterCtxKey{}).(*atomic.Int64); ok {
		resp.Body = &countingReadCloser{ReadCloser: resp.Body, counter: counter}
	}
	return resp, err
}

// countingReadCloser adds each Read's returned byte count to counter as the
// response body is consumed.
type countingReadCloser struct {
	io.ReadCloser
	counter *atomic.Int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.counter.Add(int64(n))
	return n, err
}

// newMeteredClient builds the authenticated HTTP side of the BigQuery client.
// Its RoundTripper reports consumed response body bytes via withByteCounter;
// the Storage Read gRPC side uses meteredGRPCStatsHandler above.
func newMeteredClient(ctx context.Context, creds *auth.Credentials) (*http.Client, error) {
	client, _, err := htransport.NewClient(ctx, option.WithAuthCredentials(creds))
	if err != nil {
		return nil, err
	}
	client.Transport = &meteredRoundTripper{base: client.Transport}
	return client, nil
}
