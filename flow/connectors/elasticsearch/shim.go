package connelasticsearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	opensearch "github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type searchBackend string

const (
	searchBackendElastic    searchBackend = "elasticsearch"
	searchBackendOpenSearch searchBackend = "opensearch"
)

func (b searchBackend) logPrefix() string {
	return "[" + string(b) + "]"
}

type searchAction uint8

type searchClient interface {
	Backend() searchBackend
	DiscoverNodes() error
	NewBulkIndexer(index string) (searchBulkIndexer, error)
}

type searchBulkIndexer interface {
	Add(context.Context, searchBulkIndexerItem) error
	Close(context.Context) error
	NumFlushed() uint64
}

//nolint:govet // callback-heavy helper; ordering is kept for readability.
type searchBulkIndexerItem struct {
	Body       io.ReadSeeker
	DocumentID string
	OnFailure  func(searchBulkIndexerFailure)
	OnSuccess  func()
	Action     searchAction
}

type searchBulkIndexerFailure struct {
	Err         error
	ErrorType   string
	ErrorReason string
	CauseType   string
	CauseReason string
	Status      int
}

func searchFailureToError(documentID string, failure searchBulkIndexerFailure) error {
	if failure.Err != nil {
		return failure.Err
	}

	causeString := ""
	if failure.CauseType != "" || failure.CauseReason != "" {
		causeString = fmt.Sprintf("(caused by type:%s reason:%s)", failure.CauseType, failure.CauseReason)
	}
	return fmt.Errorf("id:%s type:%s reason:%s %s",
		documentID, failure.ErrorType, failure.ErrorReason, causeString)
}

func searchHTTPTransport() http.RoundTripper {
	return &http.Transport{
		MaxIdleConnsPerHost: 4,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS13},
	}
}

//nolint:govet // auth transport state is tiny and readability matters more than packing.
type searchAuthTransport struct {
	username string
	password string
	apiKey   string
	base     http.RoundTripper
	authType protos.ElasticsearchAuthType
}

func newSearchAuthTransport(config *protos.ElasticsearchConfig, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &searchAuthTransport{
		base:     base,
		authType: config.AuthType,
		username: config.GetUsername(),
		password: config.GetPassword(),
		apiKey:   config.GetApiKey(),
	}
}

func (t *searchAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()

	switch t.authType {
	case protos.ElasticsearchAuthType_BASIC:
		cloned.SetBasicAuth(t.username, t.password)
	case protos.ElasticsearchAuthType_APIKEY:
		cloned.Header.Set("Authorization", "ApiKey "+t.apiKey)
	}

	return t.base.RoundTrip(cloned)
}

type searchInfoResponse struct {
	Version struct {
		Distribution string `json:"distribution"`
	} `json:"version"`
	Tagline string `json:"tagline"`
}

func detectSearchBackend(ctx context.Context, addresses []string, transport http.RoundTripper) (searchBackend, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("search peer has no configured addresses")
	}

	infoURL := strings.TrimRight(addresses[0], "/") + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, infoURL, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("failed to create backend probe request: %w", err)
	}

	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to probe search backend: %w", err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read backend probe response: %w", err)
	}
	if resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("failed to probe search backend from %s: %s: %s",
			infoURL, resp.Status, strings.TrimSpace(string(payload)))
	}

	if strings.EqualFold(resp.Header.Get("X-Elastic-Product"), "Elasticsearch") {
		return searchBackendElastic, nil
	}

	var info searchInfoResponse
	if err := json.Unmarshal(payload, &info); err == nil {
		if strings.EqualFold(info.Version.Distribution, "opensearch") ||
			strings.Contains(strings.ToLower(info.Tagline), "opensearch") {
			return searchBackendOpenSearch, nil
		}
	}

	return searchBackendElastic, nil
}

func newSearchClient(ctx context.Context, config *protos.ElasticsearchConfig) (searchClient, error) {
	transport := newSearchAuthTransport(config, searchHTTPTransport())
	backend, err := detectSearchBackend(ctx, config.Addresses, transport)
	if err != nil {
		return nil, err
	}

	switch backend {
	case searchBackendElastic:
		client, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: config.Addresses,
			Transport: transport,
		})
		if err != nil {
			return nil, fmt.Errorf("error creating elasticsearch connector: %w", err)
		}
		return elasticsearchClientShim{client: client}, nil
	case searchBackendOpenSearch:
		cfg := opensearch.Config{
			Addresses: config.Addresses,
			Transport: transport,
		}
		client, err := opensearch.NewClient(cfg)
		if err != nil {
			return nil, fmt.Errorf("error creating opensearch connector: %w", err)
		}
		apiClient, err := opensearchapi.NewClient(opensearchapi.Config{Client: cfg})
		if err != nil {
			return nil, fmt.Errorf("error creating opensearch API client: %w", err)
		}
		return opensearchClientShim{client: client, apiClient: apiClient}, nil
	default:
		return nil, fmt.Errorf("unsupported search backend %q", backend)
	}
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

func (a searchAction) String() string {
	switch a {
	case actionDelete:
		return "delete"
	default:
		return "index"
	}
}
