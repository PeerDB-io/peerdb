package connelasticsearch

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestSearchAuthTransportBasicAuth(t *testing.T) {
	t.Parallel()

	username := "alice"
	password := "secret"

	transport := newSearchAuthTransport(&protos.ElasticsearchConfig{
		AuthType: protos.ElasticsearchAuthType_BASIC,
		Username: &username,
		Password: &password,
	}, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		user, pass, ok := req.BasicAuth()
		require.True(t, ok)
		require.Equal(t, username, user)
		require.Equal(t, password, pass)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
		}, nil
	}))

	req, err := http.NewRequest(http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Empty(t, req.Header.Get("Authorization"))
}

func TestSearchAuthTransportAPIKey(t *testing.T) {
	t.Parallel()

	apiKey := "abc123"

	transport := newSearchAuthTransport(&protos.ElasticsearchConfig{
		AuthType: protos.ElasticsearchAuthType_APIKEY,
		ApiKey:   &apiKey,
	}, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "ApiKey "+apiKey, req.Header.Get("Authorization"))
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
		}, nil
	}))

	req, err := http.NewRequest(http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Empty(t, req.Header.Get("Authorization"))
}
