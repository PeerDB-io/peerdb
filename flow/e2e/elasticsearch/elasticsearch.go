package e2e_elasticsearch

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type elasticsearchSuite struct {
	t           *testing.T
	conn        *connpostgres.PostgresConnector
	esClient    *elasticsearch.TypedClient
	suffix      string
	esAddresses []string
}

func (s elasticsearchSuite) T() *testing.T {
	return s.t
}

func (s elasticsearchSuite) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s elasticsearchSuite) Source() e2e.SuiteSource {
	return &e2e.PostgresSource{PostgresConnector: s.conn}
}

func (s elasticsearchSuite) Suffix() string {
	return s.suffix
}

func SetupSuite(t *testing.T) elasticsearchSuite {
	t.Helper()

	suffix := "es_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")
	esAddresses := strings.Split(internal.GetEnvString("ELASTICSEARCH_TEST_ADDRESS", ""), ",")

	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: esAddresses,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 4,
		},
	})
	require.NoError(t, err, "failed to setup elasticsearch")

	return elasticsearchSuite{
		t:           t,
		conn:        conn.PostgresConnector,
		esClient:    esClient,
		esAddresses: esAddresses,
		suffix:      suffix,
	}
}

func (s elasticsearchSuite) Teardown(ctx context.Context) {
	e2e.TearDownPostgres(ctx, s)
}

func (s elasticsearchSuite) Peer() *protos.Peer {
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, "elasticsearch"),
		Type: protos.DBType_ELASTICSEARCH,
		Config: &protos.Peer_ElasticsearchConfig{
			ElasticsearchConfig: &protos.ElasticsearchConfig{
				Addresses: s.esAddresses,
				AuthType:  protos.ElasticsearchAuthType_NONE,
			},
		},
	}
	e2e.CreatePeer(s.t, ret)
	return ret
}

func (s elasticsearchSuite) countDocumentsInIndex(index string) int64 {
	res, err := s.esClient.Count().Index(index).Do(s.t.Context())
	// index may not exist yet, don't error out for that
	// search can occasionally fail, retry for that
	if err != nil && (strings.Contains(err.Error(), "index_not_found_exception") ||
		strings.Contains(err.Error(), "search_phase_execution_exception")) {
		return -1
	}
	require.NoError(s.t, err, "failed to get count of documents in index")
	return res.Count
}
