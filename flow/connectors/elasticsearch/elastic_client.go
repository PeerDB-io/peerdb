package connelasticsearch

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func newElasticsearchClient(
	config *protos.ElasticsearchConfig,
	transport http.RoundTripper,
) (elasticsearchClientShim, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: config.Addresses,
		Transport: transport,
	})
	if err != nil {
		return elasticsearchClientShim{}, fmt.Errorf("error creating elasticsearch connector: %w", err)
	}
	return elasticsearchClientShim{client: client}, nil
}

type elasticsearchClientShim struct {
	client *elasticsearch.Client
}

func (s elasticsearchClientShim) Backend() searchBackend {
	return searchBackendElastic
}

func (s elasticsearchClientShim) DiscoverNodes() error {
	return s.client.DiscoverNodes()
}

func (s elasticsearchClientShim) NewBulkIndexer(index string) (searchBulkIndexer, error) {
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         index,
		Client:        s.client,
		NumWorkers:    1,
		FlushInterval: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return elasticsearchBulkIndexerShim{indexer: indexer}, nil
}

type elasticsearchBulkIndexerShim struct {
	indexer esutil.BulkIndexer
}

func (s elasticsearchBulkIndexerShim) Add(ctx context.Context, item searchBulkIndexerItem) error {
	return s.indexer.Add(ctx, esutil.BulkIndexerItem{
		Action:     item.Action.String(),
		DocumentID: item.DocumentID,
		Body:       item.Body,
		OnSuccess: func(_ context.Context, _ esutil.BulkIndexerItem, _ esutil.BulkIndexerResponseItem) {
			if item.OnSuccess != nil {
				item.OnSuccess()
			}
		},
		OnFailure: func(_ context.Context, _ esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
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

func (s elasticsearchBulkIndexerShim) Close(ctx context.Context) error {
	return s.indexer.Close(ctx)
}

func (s elasticsearchBulkIndexerShim) NumFlushed() uint64 {
	return s.indexer.Stats().NumFlushed
}
