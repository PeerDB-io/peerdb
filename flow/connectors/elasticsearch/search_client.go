package connelasticsearch

import (
	"context"
	"fmt"
	"io"

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

func newSearchClient(ctx context.Context, config *protos.ElasticsearchConfig) (searchClient, error) {
	transport := newSearchAuthTransport(config, searchHTTPTransport())
	backend, err := detectSearchBackend(ctx, config.Addresses, transport)
	if err != nil {
		return nil, err
	}

	switch backend {
	case searchBackendElastic:
		return newElasticsearchClient(config, transport)
	case searchBackendOpenSearch:
		return newOpenSearchClient(config, transport)
	default:
		return nil, fmt.Errorf("unsupported search backend %q", backend)
	}
}

func (a searchAction) String() string {
	switch a {
	case actionDelete:
		return "delete"
	default:
		return "index"
	}
}
