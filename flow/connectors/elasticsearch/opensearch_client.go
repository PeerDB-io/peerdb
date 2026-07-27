package connelasticsearch

import (
	"context"
	"fmt"
	"net/http"
	"time"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func newOpenSearchClient(
	config *protos.ElasticsearchConfig,
	transport http.RoundTripper,
) (opensearchClientShim, error) {
	cfg := opensearch.Config{
		Addresses: config.Addresses,
		Transport: transport,
	}
	client, err := opensearch.NewClient(cfg)
	if err != nil {
		return opensearchClientShim{}, fmt.Errorf("error creating opensearch connector: %w", err)
	}
	apiClient, err := opensearchapi.NewClient(opensearchapi.Config{Client: cfg})
	if err != nil {
		return opensearchClientShim{}, fmt.Errorf("error creating opensearch API client: %w", err)
	}
	return opensearchClientShim{client: client, apiClient: apiClient}, nil
}

type opensearchClientShim struct {
	client    *opensearch.Client
	apiClient *opensearchapi.Client
}

func (s opensearchClientShim) Backend() searchBackend {
	return searchBackendOpenSearch
}

func (s opensearchClientShim) DiscoverNodes() error {
	return s.client.DiscoverNodes()
}

func (s opensearchClientShim) NewBulkIndexer(index string) (searchBulkIndexer, error) {
	indexer, err := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Index:         index,
		Client:        s.apiClient,
		NumWorkers:    1,
		FlushInterval: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return opensearchBulkIndexerShim{indexer: indexer}, nil
}

type opensearchBulkIndexerShim struct {
	indexer opensearchutil.BulkIndexer
}

func (s opensearchBulkIndexerShim) Add(ctx context.Context, item searchBulkIndexerItem) error {
	return s.indexer.Add(ctx, opensearchutil.BulkIndexerItem{
		Action:     item.Action.String(),
		DocumentID: item.DocumentID,
		Body:       item.Body,
		OnSuccess: func(_ context.Context, _ opensearchutil.BulkIndexerItem, _ opensearchapi.BulkRespItem) {
			if item.OnSuccess != nil {
				item.OnSuccess()
			}
		},
		OnFailure: func(_ context.Context, _ opensearchutil.BulkIndexerItem, res opensearchapi.BulkRespItem, err error) {
			if item.OnFailure != nil {
				item.OnFailure(searchBulkIndexerFailure{
					Err:         err,
					Status:      res.Status,
					ErrorType:   res.Error.Type,
					ErrorReason: res.Error.Reason,
					CauseType:   res.Error.Cause.Type,
					CauseReason: res.Error.Cause.Reason,
				})
			}
		},
	})
}

func (s opensearchBulkIndexerShim) Close(ctx context.Context) error {
	return s.indexer.Close(ctx)
}

func (s opensearchBulkIndexerShim) NumFlushed() uint64 {
	return s.indexer.Stats().NumFlushed
}
