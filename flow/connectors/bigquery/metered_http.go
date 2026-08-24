package connbigquery

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"

	"cloud.google.com/go/auth"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

type byteCounterCtxKey struct{}

// withByteCounter returns a context that meteredRoundTripper accumulates response
// body bytes into, for HTTP calls made using that context (including paginated
// follow-up requests, which BigQuery's RowIterator issues against the same ctx it
// was created with).
func withByteCounter(ctx context.Context, counter *atomic.Int64) context.Context {
	return context.WithValue(ctx, byteCounterCtxKey{}, counter)
}

// meteredRoundTripper counts response body bytes actually read off the wire for
// requests whose context carries a byte counter (see withByteCounter), so BigQuery
// CDC pull can report real transferred bytes instead of an approximation of the
// converted row size.
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
// response body is consumed, since chunked/compressed responses have no reliable
// Content-Length to read the size from up front.
type countingReadCloser struct {
	io.ReadCloser
	counter *atomic.Int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.counter.Add(int64(n))
	return n, err
}

// newMeteredBigQueryHTTPClient builds an authenticated HTTP client for the
// BigQuery client whose RoundTripper reports transferred bytes via withByteCounter.
func newMeteredBigQueryHTTPClient(ctx context.Context, creds *auth.Credentials) (*http.Client, error) {
	transport, err := htransport.NewTransport(ctx, &meteredRoundTripper{base: http.DefaultTransport},
		option.WithAuthCredentials(creds))
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}
