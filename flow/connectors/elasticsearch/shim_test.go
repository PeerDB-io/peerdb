package connelasticsearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestNewSearchClientDetectsElasticsearch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tagline":"You Know, for Search"}`))
	}))
	t.Cleanup(server.Close)

	client, err := newSearchClient(context.Background(), &protos.ElasticsearchConfig{
		Addresses: []string{server.URL},
		AuthType:  protos.ElasticsearchAuthType_NONE,
	})
	require.NoError(t, err)
	require.Equal(t, searchBackendElastic, client.Backend())
}

func TestNewSearchClientDetectsOpenSearch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"version":{"distribution":"opensearch"},
			"tagline":"The OpenSearch Project: https://opensearch.org/"
		}`))
	}))
	t.Cleanup(server.Close)

	client, err := newSearchClient(context.Background(), &protos.ElasticsearchConfig{
		Addresses: []string{server.URL},
		AuthType:  protos.ElasticsearchAuthType_NONE,
	})
	require.NoError(t, err)
	require.Equal(t, searchBackendOpenSearch, client.Backend())
}
